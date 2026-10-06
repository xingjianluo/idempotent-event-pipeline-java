package com.github.xingjianluo.idempotenteventpipelinejava.api;

import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.RequeueResult;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitResult;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import com.github.xingjianluo.idempotenteventpipelinejava.service.EventService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * REST API：事件提交、单条状态查询、整体统计与死信查询。
 */
@RestController
@RequestMapping("/api")
public class EventController {

    private final EventService service;

    public EventController(EventService service) {
        this.service = service;
    }

    public record SubmitRequest(String idempotencyKey, String orderKey, String payload) {
    }

    public record SubmitResponse(String outcome, String eventId, String idempotencyKey,
                                 EventStatus status, int attempts) {

        static SubmitResponse of(SubmitResult result) {
            EventRecord event = result.event();
            return new SubmitResponse(result.outcome().name(), event.getId(),
                    event.getIdempotencyKey(), event.getStatus(), event.getAttempts());
        }
    }

    /**
     * 幂等提交：首次受理返回 201，重复键复用已有事件返回 200，
     * 两种结果可通过响应体 outcome 字段区分。
     */
    @PostMapping("/events")
    public ResponseEntity<SubmitResponse> submit(@RequestBody SubmitRequest request) {
        SubmitResult result = service.submit(request.idempotencyKey(),
                request.orderKey(), request.payload());
        HttpStatus status = result.outcome() == SubmitOutcome.ACCEPTED
                ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(SubmitResponse.of(result));
    }

    @GetMapping("/events/{id}")
    public ResponseEntity<EventRecord> byId(@PathVariable String id) {
        return service.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/events/by-key/{idempotencyKey}")
    public ResponseEntity<EventRecord> byIdempotencyKey(@PathVariable String idempotencyKey) {
        return service.findByIdempotencyKey(idempotencyKey)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/stats")
    public StatsResponse stats() {
        EventStore.RequeueStats requeueStats = service.requeueStats();
        return new StatsResponse(service.stats(), requeueStats.totalRequeues(),
                requeueStats.requeuedEvents());
    }

    @GetMapping("/dead-letters")
    public List<EventRecord> deadLetters() {
        return service.deadLetters();
    }

    /**
     * 单条死信重投：生效返回 200；事件不存在 404；
     * 状态不符或超过重投配额返回 409，响应体 outcome/reason 可区分原因。
     */
    @PostMapping("/dead-letters/{id}/requeue")
    public ResponseEntity<RequeueResponse> requeue(@PathVariable String id) {
        RequeueResult result = service.requeue(id);
        HttpStatus status = switch (result.outcome()) {
            case REQUEUED -> HttpStatus.OK;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case NOT_DEAD_LETTER, QUOTA_EXCEEDED -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(RequeueResponse.of(result));
    }

    /**
     * 批量死信重投：body 可带 limit，实际生效数量被限制在配置上限
     * {@code pipeline.requeue-batch-max} 以内。
     */
    @PostMapping("/dead-letters/requeue-batch")
    public BatchRequeueResponse requeueBatch(@RequestBody(required = false) BatchRequeueRequest request) {
        Integer limit = request == null ? null : request.limit();
        List<RequeueResult> results = service.requeueBatch(limit);
        return new BatchRequeueResponse(results.stream().filter(RequeueResult::requeued).count(),
                results.size() - results.stream().filter(RequeueResult::requeued).count(),
                results.stream().map(RequeueResponse::of).toList());
    }

    public record BatchRequeueRequest(Integer limit) {
    }

    public record StatsResponse(Map<EventStatus, Long> byStatus, long totalRequeues,
                                long requeuedEvents) {
    }

    public record RequeueResponse(String outcome, String reason, String eventId,
                                  String idempotencyKey, EventStatus status,
                                  int requeueCount, int remainingRequeues) {

        static RequeueResponse of(RequeueResult result) {
            EventRecord event = result.event();
            return new RequeueResponse(result.outcome().name(), result.reason(),
                    event == null ? null : event.getId(),
                    event == null ? null : event.getIdempotencyKey(),
                    event == null ? null : event.getStatus(),
                    event == null ? 0 : event.getRequeueCount(),
                    event == null ? 0 : event.getRemainingRequeues());
        }
    }

    public record BatchRequeueResponse(long requeued, long rejected, List<RequeueResponse> results) {
    }
}
