package com.github.xingjianluo.idempotenteventpipelinejava.service;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitOutcome;
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
}
