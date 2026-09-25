package com.github.xingjianluo.idempotenteventpipelinejava.store;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.SubmitOutcome;

import java.time.Instant;
import java.util.List;

/**
 * 事件存储抽象（本地文件实现用于模拟消息代理）。
 *
 * <p>所有变更方法必须原子落盘；返回给调用方的记录均为拷贝。</p>
 */
public interface EventStore {

    /**
     * 幂等提交：相同幂等键只接受一次。
     *
     * @param now 当前时间，便于测试注入
     * @return 提交结果，携带判定结论与（已有的或新建的）事件
     */
    SubmitResult submit(String idempotencyKey, String orderKey, String payload, Instant now);

    /**
     * 领取一批当前可投递事件：
     * 仅领取每个顺序键队首、且该键当前没有 PROCESSING 事件、且已到可投递时间的事件，
     * 领取时状态置为 PROCESSING 并把 attempt +1。
     */
    List<EventRecord> claimDue(String leaseOwner, int max, Instant now);

    /** CAS 方式标记成功；仅当记录仍由该 owner 持有时生效。 */
    boolean markDelivered(long seq, String leaseOwner, Instant now);

    /** CAS 方式记录失败：未超上限则回到 PENDING 并安排退避，否则进入死信。 */
    FailureMark markFailed(long seq, String leaseOwner, String reason,
                           int maxAttempts, long nextAttemptAtMs, Instant now);

    /**
     * 启动恢复：
     * <ol>
     *   <li>把不属于当前 owner 的残留 PROCESSING（上次进程崩溃/退出时在投递）退回 PENDING；</li>
     *   <li>把所有未完成事件（PENDING）尚未到期的退避时间重置为立即可领取。</li>
     * </ol>
     * 进程重启代表一次新的投递机会，未完成事件应立即继续，而不是继续等待旧进程安排的退避。
     * 已完成（DELIVERED）与死信（DEAD_LETTER）终态绝不改动。
     *
     * @return 被恢复（状态回退或退避重置）的事件数
     */
    int recoverStaleProcessing(String currentOwner, Instant now);

    EventRecord findByKey(String idempotencyKey);

    EventRecord findBySeq(long seq);

    List<EventRecord> findAll();

    /** 提交结果。 */
    class SubmitResult {
        private final SubmitOutcome outcome;
        private final EventRecord event;

        public SubmitResult(SubmitOutcome outcome, EventRecord event) {
            this.outcome = outcome;
            this.event = event;
        }

        public SubmitOutcome getOutcome() {
            return outcome;
        }

        public EventRecord getEvent() {
            return event;
        }
    }

    /** 失败登记结果。 */
    class FailureMark {
        private final EventStatus newStatus;
        private final int attempt;

        public FailureMark(EventStatus newStatus, int attempt) {
            this.newStatus = newStatus;
            this.attempt = attempt;
        }

        public EventStatus getNewStatus() {
            return newStatus;
        }

        public int getAttempt() {
            return attempt;
        }
    }
}
