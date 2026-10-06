package com.github.xingjianluo.idempotenteventpipelinejava.service;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.model.BatchReplayResult;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.ReplayResult;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitResult;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 事件提交与查询服务。
 *
 * <p>幂等语义：以业务幂等键为唯一约束，重复提交（含并发）只会有一个请求被受理，
 * 其余请求得到 {@link SubmitOutcome#DUPLICATE} 并复用首次创建的事件记录。</p>
 */
@Service
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    private final EventStore store;
    private final PipelineProperties properties;

    public EventService(EventStore store, PipelineProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    public SubmitResult submit(String idempotencyKey, String orderKey, String payload) {
        if (!StringUtils.hasText(idempotencyKey)) {
            throw new IllegalArgumentException("idempotencyKey 不能为空");
        }
        EventRecord record = new EventRecord();
        record.setId(UUID.randomUUID().toString());
        record.setIdempotencyKey(idempotencyKey);
        record.setOrderKey(StringUtils.hasText(orderKey) ? orderKey : idempotencyKey);
        record.setPayload(payload);
        record.setStatus(EventStatus.PENDING);
        record.setAttempts(0);
        record.setMaxAttempts(properties.getMaxAttempts());
        record.setMaxReplays(properties.getMaxReplays());
        record.setNextAttemptAt(Instant.now());

        EventStore.InsertOutcome outcome = store.insertIfAbsent(record);
        SubmitResult result = new SubmitResult(
                outcome.inserted() ? SubmitOutcome.ACCEPTED : SubmitOutcome.DUPLICATE,
                outcome.record());
        log.info("提交结果: outcome={} idempotencyKey={} eventId={} status={}",
                result.outcome(), idempotencyKey, result.event().getId(), result.event().getStatus());
        return result;
    }

    public Optional<EventRecord> findById(String id) {
        return store.findById(id);
    }

    public Optional<EventRecord> findByIdempotencyKey(String idempotencyKey) {
        return store.findByIdempotencyKey(idempotencyKey);
    }

    public Map<EventStatus, Long> stats() {
        return store.countByStatus();
    }

    public List<EventRecord> deadLetters() {
        return store.findDeadLetters();
    }

    /**
     * 单条死信重投。幂等语义：重复/并发重投只有第一个请求生效，
     * 其余请求得到 {@link com.github.xingjianluo.idempotenteventpipelinejava.model.ReplayOutcome#NOT_DEAD_LETTER}。
     */
    public ReplayResult replay(String id) {
        ReplayResult result = store.replay(id, properties.getMaxReplays());
        log.info("重投结果: outcome={} eventId={} reason={}", result.outcome(), id, result.reason());
        return result;
    }

    /**
     * 批量死信重投：limit 超过配置上限时截断，避免一次重投过多挤占正常提交与投递。
     */
    public BatchReplayResult replayBatch(int requestedLimit) {
        int limit = Math.min(Math.max(requestedLimit, 0), properties.getReplayBatchMaxLimit());
        List<ReplayResult> results = store.replayBatch(limit, properties.getMaxReplays());
        int replayed = (int) results.stream().filter(ReplayResult::replayed).count();
        BatchReplayResult batch = new BatchReplayResult(limit, replayed, results.size() - replayed, results);
        log.info("批量重投结果: requested={} effectiveLimit={} replayed={} rejected={}",
                requestedLimit, limit, batch.replayed(), batch.rejected());
        return batch;
    }

    /** 整体统计：各状态数量 + 死信/重投汇总。 */
    public Map<String, Object> overview() {
        Map<String, Object> overview = new LinkedHashMap<>();
        store.countByStatus().forEach((status, count) -> overview.put(status.name(), count));
        overview.putAll(store.replayStats());
        return overview;
    }
}
