package com.github.xingjianluo.idempotenteventpipelinejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.SubmitOutcome;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于本地 JSON 文件的事件存储，用本地持久化模拟消息代理。
 *
 * <p>每次状态变更都在写锁内原子重写快照文件（先写临时文件再 move），
 * 因此崩溃最多丢失“最后一次未落盘的状态变更”；投递状态在领取/成功/失败三个关键点都会落盘，
 * 重启后由 worker 对残留 PROCESSING 做恢复，不会静默丢弃事件，也不会把未成功的事件标记为完成。</p>
 *
 * <p>线程安全：JVM 内部用 {@link ReentrantReadWriteLock} 串行化所有变更；
 * 幂等键唯一约束、FIFO 领取、状态 CAS 都在锁内判定。</p>
 */
@Component
public class FileEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(FileEventStore.class);

    private final PipelineProperties properties;
    private final ObjectMapper mapper;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** 幂等键 -> 事件，保持插入顺序，便于排查。 */
    private final Map<String, EventRecord> byKey = new LinkedHashMap<>();

    private long nextSeq = 1;
    private Path storePath;

    /**
     * 每个幂等键一把提交锁：同一键的并发提交在此排队。
     * 排队者（进入锁时发现锁已被占用）遇到已存在的在途事件时判定为“并发被拒”，
     * 与事件早已存在的普通顺序重复请求区分开。
     */
    private final ConcurrentHashMap<String, ReentrantLock> submitLocks = new ConcurrentHashMap<>();

    public FileEventStore(PipelineProperties properties) {
        this.properties = properties;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    @PostConstruct
    void init() {
        initialize();
    }

    /** 从数据目录加载（或创建）快照。非 Spring 场景（如单测）可直接调用。 */
    public void initialize() {
        try {
            Path dir = Path.of(properties.getDataDir());
            Files.createDirectories(dir);
            this.storePath = dir.resolve(properties.getStoreFile());
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("初始化事件存储失败: " + properties.getDataDir(), e);
        }
    }

    private void load() throws IOException {
        if (!Files.exists(storePath)) {
            log.info("事件存储文件不存在，从空状态启动: {}", storePath.toAbsolutePath());
            return;
        }
        Snapshot snapshot = mapper.readValue(storePath.toFile(), Snapshot.class);
        if (snapshot.events != null) {
            for (EventRecord e : snapshot.events) {
                byKey.put(e.getIdempotencyKey(), e);
                if (e.getSeq() >= nextSeq) {
                    nextSeq = e.getSeq() + 1;
                }
            }
        }
        log.info("从 {} 恢复事件 {} 条，下一个序号={}", storePath.toAbsolutePath(), byKey.size(), nextSeq);
    }

    private void persist() {
        // 调用方必须持有写锁
        try {
            Snapshot snapshot = new Snapshot(new ArrayList<>(byKey.values()), nextSeq);
            Path tmp = storePath.resolveSibling(storePath.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, storePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化事件存储失败: " + storePath, e);
        }
    }

    @Override
    public SubmitResult submit(String idempotencyKey, String orderKey, String payload, Instant now) {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        String effectiveOrderKey = (orderKey == null || orderKey.isBlank()) ? idempotencyKey : orderKey;
        ReentrantLock submitLock = submitLocks.computeIfAbsent(idempotencyKey, k -> new ReentrantLock());
        // 首个同键请求仍在提交临界区内时，后续同时到达的请求判定为“并发”；
        // 临界区已结束后才到达的重复请求判定为普通顺序重复。
        boolean concurrentPeer = submitLock.isLocked() || submitLock.getQueueLength() > 0;
        submitLock.lock();
        try {
            lock.writeLock().lock();
            try {
                EventRecord existing = byKey.get(idempotencyKey);
                if (existing != null) {
                    SubmitOutcome outcome = switch (existing.getStatus()) {
                        case DELIVERED -> SubmitOutcome.REUSED_DELIVERED;
                        case DEAD_LETTER -> SubmitOutcome.REUSED_DEAD_LETTER;
                        // 在途事件：仅当首个同键请求仍在提交临界区（并发到达）时判定为并发被拒，
                        // 否则是事件早于本次请求就已存在的普通顺序重复。
                        case PENDING, PROCESSING -> concurrentPeer
                                ? SubmitOutcome.REJECTED_CONCURRENT
                                : SubmitOutcome.DUPLICATE_INFLIGHT;
                    };
                    log.info("重复提交 idempotencyKey={} 状态={} 并发到达={} 结论={}",
                            idempotencyKey, existing.getStatus(), concurrentPeer, outcome);
                    return new SubmitResult(outcome, copy(existing));
                }
                EventRecord e = new EventRecord();
                e.setSeq(nextSeq++);
                e.setIdempotencyKey(idempotencyKey);
                e.setOrderKey(effectiveOrderKey);
                e.setPayload(payload);
                e.setStatus(EventStatus.PENDING);
                e.setAttempt(0);
                e.setCreatedAt(now);
                e.setUpdatedAt(now);
                e.setNextAttemptAt(now);
                byKey.put(idempotencyKey, e);
                persist();
                log.info("接受新事件 idempotencyKey={} orderKey={} seq={}",
                        idempotencyKey, effectiveOrderKey, e.getSeq());
                return new SubmitResult(SubmitOutcome.ACCEPTED, copy(e));
            } finally {
                lock.writeLock().unlock();
            }
        } finally {
            submitLock.unlock();
        }
    }

    @Override
    public List<EventRecord> claimDue(String leaseOwner, int max, Instant now) {
        lock.writeLock().lock();
        try {
            java.util.Set<String> busyKeys = new java.util.HashSet<>();
            Map<String, List<EventRecord>> pendingByKey = new java.util.LinkedHashMap<>();
            for (EventRecord e : byKey.values()) {
                if (e.getStatus() == EventStatus.PROCESSING) {
                    busyKeys.add(e.getOrderKey());
                } else if (e.getStatus() == EventStatus.PENDING) {
                    pendingByKey.computeIfAbsent(e.getOrderKey(), k -> new ArrayList<>()).add(e);
                }
            }
            List<EventRecord> claimed = new ArrayList<>();
            for (Map.Entry<String, List<EventRecord>> entry : pendingByKey.entrySet()) {
                if (claimed.size() >= max) {
                    break;
                }
                if (busyKeys.contains(entry.getKey())) {
                    continue; // 同键已有在途事件，保持 FIFO，不领取后续事件
                }
                entry.getValue().sort(Comparator.comparingLong(EventRecord::getSeq));
                EventRecord head = entry.getValue().get(0);
                if (head.getNextAttemptAt() != null && head.getNextAttemptAt().isAfter(now)) {
                    continue; // 退避尚未到期
                }
                head.setStatus(EventStatus.PROCESSING);
                head.setLeaseOwner(leaseOwner);
                head.setAttempt(head.getAttempt() + 1);
                head.setUpdatedAt(now);
                claimed.add(copy(head));
            }
            if (!claimed.isEmpty()) {
                persist();
                for (EventRecord e : claimed) {
                    log.info("领取事件 idempotencyKey={} orderKey={} attempt={} lease={}",
                            e.getIdempotencyKey(), e.getOrderKey(), e.getAttempt(), leaseOwner);
                }
            }
            return claimed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean markDelivered(long seq, String leaseOwner, Instant now) {
        lock.writeLock().lock();
        try {
            EventRecord e = byKey.values().stream()
                    .filter(x -> x.getSeq() == seq).findFirst().orElse(null);
            if (e == null || e.getStatus() != EventStatus.PROCESSING
                    || !Objects.equals(e.getLeaseOwner(), leaseOwner)) {
                log.warn("成功确认被拒绝 seq={} lease={} 记录={}", seq, leaseOwner,
                        e == null ? "不存在" : e.getStatus() + "/" + e.getLeaseOwner());
                return false;
            }
            e.setStatus(EventStatus.DELIVERED);
            e.setLeaseOwner(null);
            e.setNextAttemptAt(null);
            e.setUpdatedAt(now);
            persist();
            log.info("投递成功 idempotencyKey={} seq={} attempt={}",
                    e.getIdempotencyKey(), seq, e.getAttempt());
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public FailureMark markFailed(long seq, String leaseOwner, String reason,
                                  int maxAttempts, long nextAttemptAtMs, Instant now) {
        lock.writeLock().lock();
        try {
            EventRecord e = byKey.values().stream()
                    .filter(x -> x.getSeq() == seq).findFirst().orElse(null);
            if (e == null || e.getStatus() != EventStatus.PROCESSING
                    || !Objects.equals(e.getLeaseOwner(), leaseOwner)) {
                log.warn("失败登记被拒绝 seq={} lease={} 记录={}", seq, leaseOwner,
                        e == null ? "不存在" : e.getStatus() + "/" + e.getLeaseOwner());
                return null;
            }
            e.setLastFailureReason(reason);
            e.setLeaseOwner(null);
            e.setUpdatedAt(now);
            FailureMark result;
            if (e.getAttempt() >= maxAttempts) {
                e.setStatus(EventStatus.DEAD_LETTER);
                e.setNextAttemptAt(null);
                result = new FailureMark(EventStatus.DEAD_LETTER, e.getAttempt());
                log.warn("事件进入死信 idempotencyKey={} seq={} attempt={} 最后失败原因={}",
                        e.getIdempotencyKey(), seq, e.getAttempt(), reason);
            } else {
                e.setStatus(EventStatus.PENDING);
                Instant next = now.plusMillis(nextAttemptAtMs);
                e.setNextAttemptAt(next);
                result = new FailureMark(EventStatus.PENDING, e.getAttempt());
                log.warn("投递失败待重试 idempotencyKey={} seq={} attempt={} 下次尝试={}ms 后 原因={}",
                        e.getIdempotencyKey(), seq, e.getAttempt(), nextAttemptAtMs, reason);
            }
            persist();
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int recoverStaleProcessing(String currentOwner, Instant now) {
        lock.writeLock().lock();
        try {
            int count = 0;
            for (EventRecord e : byKey.values()) {
                if (e.getStatus() == EventStatus.PROCESSING
                        && !Objects.equals(e.getLeaseOwner(), currentOwner)) {
                    // 上次进程在投递中退出（kill -9 / 崩溃）：租约作废，回到待投递
                    e.setStatus(EventStatus.PENDING);
                    e.setLeaseOwner(null);
                    e.setNextAttemptAt(now);
                    e.setUpdatedAt(now);
                    count++;
                    log.warn("恢复中断事件 idempotencyKey={} seq={} 已完成尝试次数={} PROCESSING->PENDING",
                            e.getIdempotencyKey(), e.getSeq(), e.getAttempt());
                } else if (e.getStatus() == EventStatus.PENDING
                        && (e.getNextAttemptAt() == null || e.getNextAttemptAt().isAfter(now))) {
                    // 优雅退出时在途失败可能已转 PENDING 并安排了退避：重启后立即获得新的投递机会
                    e.setNextAttemptAt(now);
                    e.setUpdatedAt(now);
                    count++;
                    log.warn("重启重置退避 idempotencyKey={} seq={} 已完成尝试次数={} 立即重新可投递",
                            e.getIdempotencyKey(), e.getSeq(), e.getAttempt());
                }
            }
            if (count > 0) {
                persist();
            }
            return count;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public EventRecord findByKey(String idempotencyKey) {
        lock.readLock().lock();
        try {
            EventRecord e = byKey.get(idempotencyKey);
            return e == null ? null : copy(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public EventRecord findBySeq(long seq) {
        lock.readLock().lock();
        try {
            return byKey.values().stream().filter(e -> e.getSeq() == seq)
                    .findFirst().map(FileEventStore::copy).orElse(null);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<EventRecord> findAll() {
        lock.readLock().lock();
        try {
            return byKey.values().stream().map(FileEventStore::copy).toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    private static EventRecord copy(EventRecord src) {
        EventRecord c = new EventRecord();
        c.setSeq(src.getSeq());
        c.setIdempotencyKey(src.getIdempotencyKey());
        c.setOrderKey(src.getOrderKey());
        c.setPayload(src.getPayload());
        c.setStatus(src.getStatus());
        c.setAttempt(src.getAttempt());
        c.setCreatedAt(src.getCreatedAt());
        c.setUpdatedAt(src.getUpdatedAt());
        c.setNextAttemptAt(src.getNextAttemptAt());
        c.setLeaseOwner(src.getLeaseOwner());
        c.setLastFailureReason(src.getLastFailureReason());
        return c;
    }

    /** 落盘快照结构。 */
    private static class Snapshot {
        public List<EventRecord> events;
        public long nextSeq;

        @SuppressWarnings("unused")
        Snapshot() {
        }

        Snapshot(List<EventRecord> events, long nextSeq) {
            this.events = events;
            this.nextSeq = nextSeq;
        }
    }
}
