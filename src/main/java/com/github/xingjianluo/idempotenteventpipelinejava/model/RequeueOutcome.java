package com.github.xingjianluo.idempotenteventpipelinejava.model;

/**
 * 死信重投的结果类别。
 */
public enum RequeueOutcome {
    /** 重投生效：事件已从死信重新入队为 PENDING。 */
    REQUEUED,
    /** 事件不存在。 */
    NOT_FOUND,
    /** 事件当前不是死信（已投递成功或仍在正常处理中），拒绝重投。 */
    NOT_DEAD_LETTER,
    /** 已达到重投次数上限，拒绝重投并保留失败原因。 */
    QUOTA_EXCEEDED
}
