package com.github.xingjianluo.idempotenteventpipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 投递链路的可配置项，前缀 pipeline。
 */
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    /** 本地存储（模拟消息代理）的数据目录。 */
    private String dataDir = "data";

    /** 存储快照文件名。 */
    private String storeFile = "event-store.json";

    /** worker 轮询可投递事件的间隔（毫秒）。 */
    private long pollIntervalMs = 200;

    /** 单次事件最多投递尝试次数，达到后进入死信。 */
    private int maxAttempts = 3;

    /** 首次失败后的退避时长（毫秒）。 */
    private long backoffInitialMs = 500;

    /** 退避倍率。 */
    private double backoffMultiplier = 2.0;

    /** 退避上限（毫秒）。 */
    private long backoffMaxMs = 30_000;

    /** 单次投递最多领取并分发的事件数。 */
    private int claimBatchSize = 64;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getStoreFile() {
        return storeFile;
    }

    public void setStoreFile(String storeFile) {
        this.storeFile = storeFile;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffInitialMs() {
        return backoffInitialMs;
    }

    public void setBackoffInitialMs(long backoffInitialMs) {
        this.backoffInitialMs = backoffInitialMs;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public long getBackoffMaxMs() {
        return backoffMaxMs;
    }

    public void setBackoffMaxMs(long backoffMaxMs) {
        this.backoffMaxMs = backoffMaxMs;
    }

    public int getClaimBatchSize() {
        return claimBatchSize;
    }

    public void setClaimBatchSize(int claimBatchSize) {
        this.claimBatchSize = claimBatchSize;
    }
}
