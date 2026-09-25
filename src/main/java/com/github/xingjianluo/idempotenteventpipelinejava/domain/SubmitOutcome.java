package com.github.xingjianluo.idempotenteventpipelinejava.domain;

/**
 * 幂等提交的判定结果，用于让调用方区分“首次生效”“并发被拒”与“幂等复用”。
 */
public enum SubmitOutcome {
    /** 首次提交，事件已创建并入队。 */
    ACCEPTED,
    /** 相同幂等键已有另一个并发请求先占位，本次请求未生效。 */
    REJECTED_CONCURRENT,
    /** 相同幂等键的事件此前已提交但尚未完成，本次为顺序重复请求，未生效。 */
    DUPLICATE_INFLIGHT,
    /** 相同幂等键的事件此前已完成投递，本次请求直接复用已有结果。 */
    REUSED_DELIVERED,
    /** 相同幂等键的事件此前已进入死信，本次请求复用该终态结果。 */
    REUSED_DEAD_LETTER
}
