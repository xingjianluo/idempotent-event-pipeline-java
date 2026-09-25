package com.github.xingjianluo.idempotenteventpipelinejava.store;

import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;

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

    /** 插入结果：区分“新建”与“命中已有幂等键”。 */
    record InsertOutcome(boolean inserted, EventRecord record) {
    }
}
