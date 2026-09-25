package com.github.xingjianluo.idempotenteventpipelinejava.model;

/**
 * 事件投递生命周期状态。
 */
public enum EventStatus {
    /** 已接收，等待投递（含等待重试）。 */
    PENDING,
    /** 已被调度器认领，正在投递中。 */
    IN_FLIGHT,
    /** 投递成功，终态。 */
    DELIVERED,
    /** 超过最大重试次数，进入死信，终态。 */
    DEAD_LETTER
}
