package com.github.xingjianluo.idempotenteventpipelinejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.ReplayOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.ReplayResult;
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
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
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
            return byId.values().stream()
                    .filter(r -> r.getStatus() == EventStatus.PENDING)
                    .filter(r -> !r.getNextAttemptAt().isAfter(now))
                    .sorted(Comparator.comparing(EventRecord::getCreatedAt))
                    .limit(limit)
                    .toList();
        }
    }

    @Override
    public List<EventRecord> findDispatchableHeads(Instant now, int limit) {
        synchronized (lock) {
            // 每个顺序键只保留最早的一条 PENDING 作为队首（createdAt 相同则按插入序）。
            Map<String, EventRecord> headByOrderKey = new LinkedHashMap<>();
            byId.values().stream()
                    .filter(r -> r.getStatus() == EventStatus.PENDING)
                    .sorted(Comparator.comparing(EventRecord::getCreatedAt))
                    .forEach(r -> headByOrderKey.putIfAbsent(r.getOrderKey(), r));
            // 队首未到期（退避等待中）则整键阻塞，后续事件不得越过。
            return headByOrderKey.values().stream()
                    .filter(r -> !r.getNextAttemptAt().isAfter(now))
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
    public ReplayResult replay(String id, int maxReplays) {
        synchronized (lock) {
            EventRecord record = byId.get(id);
            if (record == null) {
                log.info("重投被拒绝: id={} 原因=事件不存在", id);
                return new ReplayResult(ReplayOutcome.NOT_FOUND, null, "事件不存在: " + id);
            }
            if (record.getStatus() != EventStatus.DEAD_LETTER) {
                // 并发/重复重投时，只有第一个请求能把事件从死信重置走，其余走到这里被拒绝。
                String reason = "仅死信事件可重投，当前状态为 " + record.getStatus();
                log.info("重投被拒绝: id={} idempotencyKey={} status={} 原因={}",
                        record.getId(), record.getIdempotencyKey(), record.getStatus(), reason);
                return new ReplayResult(ReplayOutcome.NOT_DEAD_LETTER, record, reason);
            }
            int effectiveMax = effectiveMaxReplays(record, maxReplays);
            if (record.getReplayCount() >= effectiveMax) {
                String reason = "重投次数已达上限: replayCount=" + record.getReplayCount()
                        + " maxReplays=" + effectiveMax;
                record.setLastReplayRejectReason(reason);
                record.setVersion(record.getVersion() + 1);
                record.setUpdatedAt(Instant.now());
                persist(record);
                log.warn("重投被拒绝（超限）: id={} idempotencyKey={} replayCount={} maxReplays={}",
                        record.getId(), record.getIdempotencyKey(),
                        record.getReplayCount(), effectiveMax);
                return new ReplayResult(ReplayOutcome.QUOTA_EXCEEDED, record, reason);
            }
            // 重新入队：保留原 createdAt 以维持同顺序键中的原始位置，开启新一轮投递尝试。
            EventStatus from = record.getStatus();
            record.setStatus(EventStatus.PENDING);
            record.setAttempts(0);
            record.setNextAttemptAt(Instant.now());
            record.setReplayCount(record.getReplayCount() + 1);
            record.setLastReplayAt(record.getNextAttemptAt());
            record.setLastReplayRejectReason(null);
            record.setVersion(record.getVersion() + 1);
            record.setUpdatedAt(Instant.now());
            persist(record);
            log.info("死信重投生效: id={} idempotencyKey={} orderKey={} replayCount={}/{} {} -> {}",
                    record.getId(), record.getIdempotencyKey(), record.getOrderKey(),
                    record.getReplayCount(), effectiveMax, from, record.getStatus());
            return new ReplayResult(ReplayOutcome.REPLAYED, record, null);
        }
    }

    /** 老数据（重投字段缺失，maxReplays=0）回退到当前配置值。 */
    private int effectiveMaxReplays(EventRecord record, int configuredMaxReplays) {
        return record.getMaxReplays() > 0 ? record.getMaxReplays() : configuredMaxReplays;
    }

    @Override
    public List<ReplayResult> replayBatch(int limit, int maxReplays) {
        synchronized (lock) {
            List<EventRecord> dead = byId.values().stream()
                    .filter(r -> r.getStatus() == EventStatus.DEAD_LETTER)
                    .sorted(Comparator.comparing(EventRecord::getCreatedAt))
                    .toList();
            List<ReplayResult> results = new ArrayList<>();
            int replayed = 0;
            for (EventRecord record : dead) {
                if (replayed >= limit) {
                    break; // 达到批量上限，剩余死信留待下次，避免长时间占用存储锁
                }
                ReplayResult result = replay(record.getId(), maxReplays);
                results.add(result);
                if (result.replayed()) {
                    replayed++;
                }
            }
            log.info("批量重投完成: limit={} 生效={} 拒绝={}", limit, replayed, results.size() - replayed);
            return results;
        }
    }

    @Override
    public Map<String, Long> replayStats() {
        synchronized (lock) {
            long totalReplays = 0;
            long eventsReplayed = 0;
            for (EventRecord record : byId.values()) {
                totalReplays += record.getReplayCount();
                if (record.getReplayCount() > 0) {
                    eventsReplayed++;
                }
            }
            return Map.of("totalReplays", totalReplays, "eventsReplayed", eventsReplayed);
        }
    }
}
