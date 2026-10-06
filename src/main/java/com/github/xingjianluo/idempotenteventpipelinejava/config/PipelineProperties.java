package com.github.xingjianluo.idempotenteventpipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 投递链路可配置项。
 */
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    /** 本地事件存储文件路径（模拟消息代理）。 */
    private String storePath = "data/events.jsonl";

    /** 单条事件最大投递尝试次数，超过后进入死信。 */
    private int maxAttempts = 3;

    /** 重试退避基数（毫秒）。 */
    private long backoffBaseMs = 200;

    /** 重试退避上限（毫秒）。 */
    private long backoffMaxMs = 5000;

    /** 调度器轮询间隔（毫秒）。 */
    private long pollIntervalMs = 100;

    /** 单条死信事件最大重投次数，超过后拒绝重投。 */
    private int maxReplays = 3;

    /** 批量重投单次允许的最大条数上限（请求参数超过时按此值截断）。 */
    private int replayBatchMaxLimit = 100;

    public String getStorePath() {
        return storePath;
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffBaseMs() {
        return backoffBaseMs;
    }

    public void setBackoffBaseMs(long backoffBaseMs) {
        this.backoffBaseMs = backoffBaseMs;
    }

    public long getBackoffMaxMs() {
        return backoffMaxMs;
    }

    public void setBackoffMaxMs(long backoffMaxMs) {
        this.backoffMaxMs = backoffMaxMs;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public int getMaxReplays() {
        return maxReplays;
    }

    public void setMaxReplays(int maxReplays) {
        this.maxReplays = maxReplays;
    }

    public int getReplayBatchMaxLimit() {
        return replayBatchMaxLimit;
    }

    public void setReplayBatchMaxLimit(int replayBatchMaxLimit) {
        this.replayBatchMaxLimit = replayBatchMaxLimit;
    }
}
