package com.github.xingjianluo.idempotenteventpipelinejava.model;

import java.time.Instant;

/**
 * 一条待投递事件的持久化记录。
 */
public class EventRecord {

    private String id;
    private String idempotencyKey;
    private String orderKey;
    private String payload;
    private EventStatus status;
    private int attempts;
    private int maxAttempts;
    private String lastError;
    private Instant nextAttemptAt;
    private Instant createdAt;
    private Instant updatedAt;
    private long version;
    /** 已生效的死信重投次数。 */
    private int requeueCount;
    /** 允许的最大重投次数（提交时从配置快照）。 */
    private int maxRequeues;
    /** 最近一次重投生效时间。 */
    private Instant lastRequeuedAt;

    public EventRecord() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public void setNextAttemptAt(Instant nextAttemptAt) {
        this.nextAttemptAt = nextAttemptAt;
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

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public int getRequeueCount() {
        return requeueCount;
    }

    public void setRequeueCount(int requeueCount) {
        this.requeueCount = requeueCount;
    }

    public int getMaxRequeues() {
        return maxRequeues;
    }

    public void setMaxRequeues(int maxRequeues) {
        this.maxRequeues = maxRequeues;
    }

    public Instant getLastRequeuedAt() {
        return lastRequeuedAt;
    }

    public void setLastRequeuedAt(Instant lastRequeuedAt) {
        this.lastRequeuedAt = lastRequeuedAt;
    }

    /** 剩余可重投次数（基于记录内快照的 maxRequeues 计算，不为负）。 */
    public int getRemainingRequeues() {
        return Math.max(0, maxRequeues - requeueCount);
    }
}
