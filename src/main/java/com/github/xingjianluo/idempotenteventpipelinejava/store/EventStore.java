package com.github.xingjianluo.idempotenteventpipelinejava.store;

import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.RequeueResult;

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
     * 单条死信重投（幂等）：仅当事件处于 DEAD_LETTER 且重投次数未超上限时生效，
     * 原子地重置为 PENDING 并累加重投次数；其余情况返回可区分的拒绝结果。
     *
     * @param maxRequeues 该事件允许的最大重投次数
     */
    RequeueResult requeue(String id, int maxRequeues);

    /**
     * 批量死信重投：按提交先后最多重投 limit 条死信（已超配额的跳过且不占额度）。
     *
     * @return 每条被处理死信的结果（含生效与配额拒绝）
     */
    List<RequeueResult> requeueBatch(int limit, int maxRequeues);

    /** 重投统计：累计生效重投次数与发生过重投的事件数。 */
    RequeueStats requeueStats();

    /** 插入结果：区分“新建”与“命中已有幂等键”。 */
    record InsertOutcome(boolean inserted, EventRecord record) {
    }

    /** 重投统计快照。 */
    record RequeueStats(long totalRequeues, long requeuedEvents) {
    }
}
