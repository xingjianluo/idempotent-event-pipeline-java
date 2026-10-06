package com.github.xingjianluo.idempotenteventpipelinejava.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.RequeueOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.RequeueResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于本地 JSONL 文件的事件存储，模拟消息代理的持久化语义。
 *
 * <p>写入模型：内存索引（byId / byIdempotencyKey）+ 每次状态变更向文件追加一条完整快照；
 * 启动时按行重放、后写覆盖先写，从而恢复到崩溃前的状态。
 * 所有公开方法在同一个监视器上串行化，保证单进程内的原子性与幂等键唯一约束。</p>
 */
public class FileEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(FileEventStore.class);

    private final Path file;
    private final ObjectMapper mapper;
    private final Object lock = new Object();

    private final Map<String, EventRecord> byId = new LinkedHashMap<>();
    private final Map<String, String> idByIdempotencyKey = new LinkedHashMap<>();

    public FileEventStore(Path file) {
        this.file = file;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                // 容忍派生属性（如 remainingRequeues）与历史快照中缺失/多余的字段
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        load();
    }

    private void load() {
        try {
            if (Files.notExists(file)) {
                Path parent = file.toAbsolutePath().getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.createFile(file);
                log.info("事件存储文件已创建: {}", file.toAbsolutePath());
                return;
            }
            int replayed = 0;
            try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
                for (String line : (Iterable<String>) lines::iterator) {
                    if (line.isBlank()) {
                        continue;
                    }
                    EventRecord record = mapper.readValue(line, EventRecord.class);
                    apply(record);
                    replayed++;
                }
            }
            log.info("事件存储恢复完成: 文件={} 重放记录数={} 当前事件数={}",
                    file.toAbsolutePath(), replayed, byId.size());
        } catch (IOException e) {
            throw new UncheckedIOException("加载事件存储失败: " + file, e);
        }
    }

    private void apply(EventRecord record) {
        byId.put(record.getId(), record);
        idByIdempotencyKey.put(record.getIdempotencyKey(), record.getId());
    }

    private void persist(EventRecord record) {
        try {
            String line = mapper.writeValueAsString(record) + System.lineSeparator();
            Files.writeString(file, line, StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND, StandardOpenOption.SYNC);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化事件失败: " + record.getId(), e);
        }
    }

    @Override
    public InsertOutcome insertIfAbsent(EventRecord record) {
        synchronized (lock) {
            String existingId = idByIdempotencyKey.get(record.getIdempotencyKey());
            if (existingId != null) {
                log.info("幂等键冲突，复用已有事件: idempotencyKey={} existingId={}",
                        record.getIdempotencyKey(), existingId);
                return new InsertOutcome(false, byId.get(existingId));
            }
            record.setVersion(0L);
            record.setCreatedAt(Instant.now());
            record.setUpdatedAt(record.getCreatedAt());
            apply(record);
            persist(record);
            log.info("新事件已受理: id={} idempotencyKey={} orderKey={} status={}",
                    record.getId(), record.getIdempotencyKey(), record.getOrderKey(), record.getStatus());
            return new InsertOutcome(true, record);
        }
    }

    @Override
    public Optional<EventRecord> findById(String id) {
        synchronized (lock) {
            return Optional.ofNullable(byId.get(id));
        }
    }

    @Override
    public Optional<EventRecord> findByIdempotencyKey(String idempotencyKey) {
        synchronized (lock) {
            String id = idByIdempotencyKey.get(idempotencyKey);
            return id == null ? Optional.empty() : Optional.of(byId.get(id));
        }
    }

    @Override
    public List<EventRecord> findDue(Instant now, int limit) {
        synchronized (lock) {
            // 每个顺序键内最早的未完成（PENDING/IN_FLIGHT）事件创建时间：
            // 同键后续事件必须等它成功或进死信后才能被调度，哪怕它还在退避等待中。
            Map<String, Instant> headByOrderKey = new LinkedHashMap<>();
            for (EventRecord r : byId.values()) {
                if (r.getStatus() == EventStatus.PENDING || r.getStatus() == EventStatus.IN_FLIGHT) {
                    headByOrderKey.merge(r.getOrderKey(), r.getCreatedAt(),
                            (a, b) -> a.isBefore(b) ? a : b);
                }
            }
            return byId.values().stream()
                    .filter(r -> r.getStatus() == EventStatus.PENDING)
                    .filter(r -> !r.getNextAttemptAt().isAfter(now))
                    .filter(r -> {
                        Instant head = headByOrderKey.get(r.getOrderKey());
                        return head == null || !head.isBefore(r.getCreatedAt());
                    })
                    .sorted(Comparator.comparing(EventRecord::getCreatedAt))
                    .limit(limit)
                    .toList();
        }
    }

    @Override
    public boolean claim(String id, long expectedVersion) {
        synchronized (lock) {
            EventRecord record = byId.get(id);
            if (record == null || record.getStatus() != EventStatus.PENDING
                    || record.getVersion() != expectedVersion) {
                return false;
            }
            transition(record, EventStatus.IN_FLIGHT, record.getAttempts(), record.getLastError(),
                    record.getNextAttemptAt());
            return true;
        }
    }

    @Override
    public void markDelivered(String id, long expectedVersion) {
        synchronized (lock) {
            EventRecord record = requireInFlight(id, expectedVersion);
            transition(record, EventStatus.DELIVERED, record.getAttempts(), record.getLastError(),
                    record.getNextAttemptAt());
        }
    }

    @Override
    public void reschedule(String id, long expectedVersion, int attempts, String lastError, Instant nextAttemptAt) {
        synchronized (lock) {
            EventRecord record = requireInFlight(id, expectedVersion);
            transition(record, EventStatus.PENDING, attempts, lastError, nextAttemptAt);
        }
    }

    @Override
    public void markDead(String id, long expectedVersion, int attempts, String lastError) {
        synchronized (lock) {
            EventRecord record = requireInFlight(id, expectedVersion);
            transition(record, EventStatus.DEAD_LETTER, attempts, lastError, record.getNextAttemptAt());
        }
    }

    private EventRecord requireInFlight(String id, long expectedVersion) {
        EventRecord record = byId.get(id);
        if (record == null || record.getStatus() != EventStatus.IN_FLIGHT
                || record.getVersion() != expectedVersion) {
            throw new IllegalStateException("事件不在 IN_FLIGHT 或版本不匹配: id=" + id);
        }
        return record;
    }

    private void transition(EventRecord record, EventStatus target, int attempts,
                            String lastError, Instant nextAttemptAt) {
        EventStatus from = record.getStatus();
        record.setStatus(target);
        record.setAttempts(attempts);
        record.setLastError(lastError);
        record.setNextAttemptAt(nextAttemptAt);
        record.setVersion(record.getVersion() + 1);
        record.setUpdatedAt(Instant.now());
        persist(record);
        log.info("事件状态迁移: id={} idempotencyKey={} attempts={} {} -> {}{}",
                record.getId(), record.getIdempotencyKey(), attempts, from, target,
                lastError == null ? "" : " lastError=" + lastError);
    }

    @Override
    public List<EventRecord> recoverInFlight() {
        synchronized (lock) {
            List<EventRecord> recovered = new ArrayList<>();
            for (EventRecord record : byId.values()) {
                if (record.getStatus() == EventStatus.IN_FLIGHT) {
                    transition(record, EventStatus.PENDING, record.getAttempts(),
                            record.getLastError(), Instant.now());
                    recovered.add(record);
                }
            }
            if (!recovered.isEmpty()) {
                log.info("重启恢复: {} 条 IN_FLIGHT 事件已重置为 PENDING", recovered.size());
            }
            return recovered;
        }
    }

    @Override
    public Map<EventStatus, Long> countByStatus() {
        synchronized (lock) {
            Map<EventStatus, Long> counts = new EnumMap<>(EventStatus.class);
            for (EventStatus status : EventStatus.values()) {
                counts.put(status, 0L);
            }
            for (EventRecord record : byId.values()) {
                counts.merge(record.getStatus(), 1L, Long::sum);
            }
            return counts;
        }
    }

    @Override
    public List<EventRecord> findDeadLetters() {
        synchronized (lock) {
            return byId.values().stream()
                    .filter(r -> r.getStatus() == EventStatus.DEAD_LETTER)
                    .toList();
        }
    }

    @Override
    public RequeueResult requeue(String id, int maxRequeues) {
        synchronized (lock) {
            EventRecord record = byId.get(id);
            if (record == null) {
                log.info("重投拒绝: id={} 原因=事件不存在", id);
                return new RequeueResult(RequeueOutcome.NOT_FOUND, null, "事件不存在: id=" + id);
            }
            return doRequeue(record, maxRequeues);
        }
    }

    @Override
    public List<RequeueResult> requeueBatch(int limit, int maxRequeues) {
        synchronized (lock) {
            List<RequeueResult> results = new ArrayList<>();
            List<EventRecord> dead = byId.values().stream()
                    .filter(r -> r.getStatus() == EventStatus.DEAD_LETTER)
                    .sorted(Comparator.comparing(EventRecord::getCreatedAt))
                    .toList();
            int requeued = 0;
            for (EventRecord record : dead) {
                if (requeued >= limit) {
                    break;
                }
                RequeueResult result = doRequeue(record, maxRequeues);
                if (result.outcome() == RequeueOutcome.REQUEUED) {
                    requeued++;
                    results.add(result);
                } else if (result.outcome() == RequeueOutcome.QUOTA_EXCEEDED) {
                    // 超配额的死信跳过且不占批量额度，结果一并返回便于观察
                    results.add(result);
                }
            }
            log.info("批量重投完成: limit={} 生效={} 配额拒绝={}",
                    limit, requeued, results.size() - requeued);
            return results;
        }
    }

    /** 在锁内执行单条重投校验与状态迁移，保证并发/重复重投只生效一次。 */
    private RequeueResult doRequeue(EventRecord record, int maxRequeues) {
        if (record.getStatus() != EventStatus.DEAD_LETTER) {
            String reason = "仅死信可重投，当前状态=" + record.getStatus();
            log.info("重投拒绝: id={} idempotencyKey={} 原因={}",
                    record.getId(), record.getIdempotencyKey(), reason);
            return new RequeueResult(RequeueOutcome.NOT_DEAD_LETTER, record, reason);
        }
        if (record.getRequeueCount() >= maxRequeues) {
            String reason = "重投次数已达上限: requeueCount=" + record.getRequeueCount()
                    + " maxRequeues=" + maxRequeues;
            log.warn("重投拒绝: id={} idempotencyKey={} 原因={} lastError={}",
                    record.getId(), record.getIdempotencyKey(), reason, record.getLastError());
            return new RequeueResult(RequeueOutcome.QUOTA_EXCEEDED, record, reason);
        }
        // 重新入队：回到 PENDING 立即参与调度，attempts 归零获得新一轮重试额度，
        // createdAt 保持不变，按原来的先后位置参与同顺序键排队；lastError 保留。
        record.setRequeueCount(record.getRequeueCount() + 1);
        record.setLastRequeuedAt(Instant.now());
        transition(record, EventStatus.PENDING, 0, record.getLastError(), Instant.now());
        log.info("死信重投生效: id={} idempotencyKey={} orderKey={} requeueCount={}/{}",
                record.getId(), record.getIdempotencyKey(), record.getOrderKey(),
                record.getRequeueCount(), maxRequeues);
        return new RequeueResult(RequeueOutcome.REQUEUED, record, null);
    }

    @Override
    public RequeueStats requeueStats() {
        synchronized (lock) {
            long total = 0;
            long events = 0;
            for (EventRecord r : byId.values()) {
                if (r.getRequeueCount() > 0) {
                    events++;
                    total += r.getRequeueCount();
                }
            }
            return new RequeueStats(total, events);
        }
    }
}
