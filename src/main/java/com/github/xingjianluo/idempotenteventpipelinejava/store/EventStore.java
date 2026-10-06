package com.github.xingjianluo.idempotenteventpipelinejava.store;

import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.ReplayResult;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 事件存储抽象：本地存储模拟消息代理的持久化语义。
 */
public interface EventStore {

    /**
     * 以幂等键唯一约束插入新事件。
     *
     * @return 插入成功返回新记录；幂等键已存在时返回已有记录（不插入）。
     */
    InsertOutcome insertIfAbsent(EventRecord record);

    Optional<EventRecord> findById(String id);

    Optional<EventRecord> findByIdempotencyKey(String idempotencyKey);

    /** 拉取到期可投递的 PENDING 事件。 */
    List<EventRecord> findDue(Instant now, int limit);

    /**
     * 拉取可调度的事件：每个顺序键只取最早的一条 PENDING（队首），且仅当队首已到期才返回。
     * 队首未到期（如正在退避等待）时，该顺序键的后续事件一律不返回，保证同键队首阻塞。
     */
    List<EventRecord> findDispatchableHeads(Instant now, int limit);

    /** 乐观锁认领：PENDING -> IN_FLIGHT，成功返回 true。 */
    boolean claim(String id, long expectedVersion);

    /** IN_FLIGHT -> DELIVERED。 */
    void markDelivered(String id, long expectedVersion);

    /** IN_FLIGHT -> PENDING（重试），记录失败原因与下次尝试时间。 */
    void reschedule(String id, long expectedVersion, int attempts, String lastError, Instant nextAttemptAt);

    /** IN_FLIGHT -> DEAD_LETTER，保留最后一次失败原因。 */
    void markDead(String id, long expectedVersion, int attempts, String lastError);

    /** 重启恢复：把残留的 IN_FLIGHT 事件重置为 PENDING，返回受影响记录。 */
    List<EventRecord> recoverInFlight();

    Map<EventStatus, Long> countByStatus();

    List<EventRecord> findDeadLetters();

    /**
     * 单条死信重投：仅当事件处于 DEAD_LETTER 且重投次数未达上限时，
     * 原子地将其重置为 PENDING（保留原 createdAt 以维持原顺序位），replayCount 加一。
     * 重复/并发重投只有第一个请求生效，其余得到可区分的拒绝结果。
     */
    ReplayResult replay(String id, int maxReplays);

    /**
     * 批量死信重投：按创建时间从早到晚最多重投 limit 条死信，
     * 每条独立校验状态与配额，单条失败不影响其他条。
     */
    List<ReplayResult> replayBatch(int limit, int maxReplays);

    /** 重投统计：totalReplays=累计生效重投次数，eventsReplayed=被重投过的事件数。 */
    Map<String, Long> replayStats();

    /** 插入结果：区分“新建”与“命中已有幂等键”。 */
    record InsertOutcome(boolean inserted, EventRecord record) {
    }
}
