package com.github.xingjianluo.idempotenteventpipelinejava.model;

/**
 * 单条死信重投结果：调用方可区分“生效”与各类拒绝原因。
 *
 * @param outcome 结果类别
 * @param event 事件快照（事件不存在时为 null）
 * @param reason 拒绝原因说明；成功时为 null
 */
public record RequeueResult(RequeueOutcome outcome, EventRecord event, String reason) {

    public boolean requeued() {
        return outcome == RequeueOutcome.REQUEUED;
    }
}
