package com.github.xingjianluo.idempotenteventpipelinejava.domain;

import java.time.Instant;

/**
 * 一条事件的持久化记录，也是本地“消息代理”存储中的最小数据单元。
 *
 * <p>可变字段通过加锁的存储层统一修改，跨层传递时使用拷贝。</p>
 */
public class EventRecord {

    /** 存储层分配的自增序号，同时承担同顺序键下的 FIFO 排序职责。 */
    private long seq;

    /** 业务幂等键，全库唯一。 */
    private String idempotencyKey;

    /** 顺序键：相同键的事件严格按提交顺序串行投递。 */
    private String orderKey;

    /** 事件载荷（JSON 字符串或任意文本）。 */
    private String payload;

    private EventStatus status;

    /** 已发起的投递尝试次数：领取投递时计为第 1 次。 */
    private int attempt;

    /** 入队时间。 */
    private Instant createdAt;

    /** 最近一次状态变更时间。 */
    private Instant updatedAt;

    /** 变为可投递（首次或重试）的最早时间，未到期前不会被领取。 */
    private Instant nextAttemptAt;

    /** 当前持有该事件的 worker 标识；非 PROCESSING 时为 null。 */
    private String leaseOwner;

    /** 最后一次投递失败原因（异常类名 + message）。 */
    private String lastFailureReason;

    public EventRecord() {
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public void setNextAttemptAt(Instant nextAttemptAt) {
        this.nextAttemptAt = nextAttemptAt;
    }

    public String getLeaseOwner() {
        return leaseOwner;
    }

    public void setLeaseOwner(String leaseOwner) {
        this.leaseOwner = leaseOwner;
    }

    public String getLastFailureReason() {
        return lastFailureReason;
    }

    public void setLastFailureReason(String lastFailureReason) {
        this.lastFailureReason = lastFailureReason;
    }
}
