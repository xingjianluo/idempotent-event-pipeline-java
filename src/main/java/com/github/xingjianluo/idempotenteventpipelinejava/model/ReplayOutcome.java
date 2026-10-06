package com.github.xingjianluo.idempotenteventpipelinejava.model;

/**
 * 死信重投的结果类别。
 */
public enum ReplayOutcome {
    /** 重投生效：事件已重新入队（DEAD_LETTER -> PENDING）。 */
    REPLAYED,
    /** 事件不存在。 */
    NOT_FOUND,
    /** 当前状态不是死信，拒绝重投（含并发/重复重投时另一请求先生效的情况）。 */
    NOT_DEAD_LETTER,
    /** 已达到重投次数上限，拒绝重投。 */
    QUOTA_EXCEEDED
}
