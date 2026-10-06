package com.github.xingjianluo.idempotenteventpipelinejava.service;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.RequeueResult;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitResult;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
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
        record.setMaxRequeues(properties.getMaxRequeues());
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
     * 单条死信重投。重投动作幂等：并发/重复调用只生效一次，
     * 其余请求得到可区分的拒绝结果（状态不符或配额超限）。
     */
    public RequeueResult requeue(String id) {
        RequeueResult result = store.requeue(id, effectiveMaxRequeues(id));
        log.info("重投结果: outcome={} eventId={} requeueCount={} reason={}",
                result.outcome(), id,
                result.event() == null ? "-" : result.event().getRequeueCount(),
                result.reason());
        return result;
    }

    /**
     * 批量死信重投：实际数量被限制在 {@code pipeline.requeue-batch-max} 以内，
     * 避免一次重投过多挤占正常新事件的提交与投递。
     *
     * @param requestedLimit 请求的批量上限；为空或小于 1 时取配置上限
     */
    public List<RequeueResult> requeueBatch(Integer requestedLimit) {
        int limit = requestedLimit == null || requestedLimit < 1
                ? properties.getRequeueBatchMax()
                : Math.min(requestedLimit, properties.getRequeueBatchMax());
        List<RequeueResult> results = store.requeueBatch(limit, properties.getMaxRequeues());
        long requeued = results.stream().filter(RequeueResult::requeued).count();
        log.info("批量重投结果: 请求上限={} 生效上限={} 生效={} 拒绝={}",
                requestedLimit, limit, requeued, results.size() - requeued);
        return results;
    }

    public EventStore.RequeueStats requeueStats() {
        return store.requeueStats();
    }

    /** 历史快照中 maxRequeues 可能缺失（为 0），此时回退到当前配置值。 */
    private int effectiveMaxRequeues(String id) {
        return store.findById(id)
                .map(r -> r.getMaxRequeues() > 0 ? r.getMaxRequeues() : properties.getMaxRequeues())
                .orElse(properties.getMaxRequeues());
    }
}
