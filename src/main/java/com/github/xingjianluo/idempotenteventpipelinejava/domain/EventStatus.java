package com.github.xingjianluo.idempotenteventpipelinejava.domain;

/**
 * 事件在投递链路中的生命周期状态。
 *
 * <ul>
 *   <li>{@link #PENDING}：已提交，尚未完成投递（含等待下一次重试）。</li>
 *   <li>{@link #PROCESSING}：已被某个 worker 领取，正在调用下游处理器。</li>
 *   <li>{@link #DELIVERED}：下游确认成功，终态。</li>
 *   <li>{@link #DEAD_LETTER}：重试次数耗尽后进入死信，终态，保留最后一次失败原因。</li>
 * </ul>
 */
public enum EventStatus {
    PENDING,
    PROCESSING,
    DELIVERED,
    DEAD_LETTER
}
