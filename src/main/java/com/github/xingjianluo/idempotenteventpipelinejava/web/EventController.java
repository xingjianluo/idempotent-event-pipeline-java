package com.github.xingjianluo.idempotenteventpipelinejava.web;

import com.github.xingjianluo.idempotenteventpipelinejava.handler.SimulatedDeliveryHandler;
import com.github.xingjianluo.idempotenteventpipelinejava.service.EventService;
import com.github.xingjianluo.idempotenteventpipelinejava.service.ObservabilityService;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import com.github.xingjianluo.idempotenteventpipelinejava.web.dto.EventStats;
import com.github.xingjianluo.idempotenteventpipelinejava.web.dto.EventView;
import com.github.xingjianluo.idempotenteventpipelinejava.web.dto.RuleRequest;
import com.github.xingjianluo.idempotenteventpipelinejava.web.dto.SubmitRequest;
import com.github.xingjianluo.idempotenteventpipelinejava.web.dto.SubmitResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 事件投递链路的 REST 入口与观测接口。
 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventService eventService;
    private final ObservabilityService observability;

    public EventController(EventService eventService, ObservabilityService observability) {
        this.eventService = eventService;
        this.observability = observability;
    }

    /**
     * 提交事件。
     *
     * <ul>
     *   <li>201 ACCEPTED：首次生效；</li>
     *   <li>409 REJECTED_CONCURRENT：同键并发提交，本次被拒；</li>
     *   <li>409 DUPLICATE_INFLIGHT：同键事件已在处理中，本次为顺序重复；</li>
     *   <li>200 REUSED_DELIVERED / REUSED_DEAD_LETTER：幂等复用终态结果。</li>
     * </ul>
     */
    @PostMapping
    public ResponseEntity<SubmitResponse> submit(@RequestBody SubmitRequest request) {
        if (request == null || request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new IllegalArgumentException("idempotencyKey 不能为空");
        }
        EventStore.SubmitResult result = eventService.submit(
                request.idempotencyKey(), request.orderKey(), request.payload());
        SubmitResponse body = new SubmitResponse(result.getOutcome().name(), EventView.from(result.getEvent()));
        return switch (result.getOutcome()) {
            case ACCEPTED -> ResponseEntity.status(HttpStatus.CREATED).body(body);
            case REJECTED_CONCURRENT, DUPLICATE_INFLIGHT ->
                    ResponseEntity.status(HttpStatus.CONFLICT).body(body);
            case REUSED_DELIVERED, REUSED_DEAD_LETTER -> ResponseEntity.ok(body);
        };
    }

    /** 查询单条事件当前状态（含尝试次数、退避时间与最后失败原因）。 */
    @GetMapping("/{idempotencyKey}")
    public ResponseEntity<EventView> get(@PathVariable String idempotencyKey) {
        EventView view = observability.getEvent(idempotencyKey);
        return view == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(view);
    }

    /** 整体投递统计。 */
    @GetMapping("/stats")
    public EventStats stats() {
        return observability.stats();
    }

    /** 测试/本地验证用：为某幂等键配置前 N 次投递失败。 */
    @PostMapping("/admin/fail-rules")
    public ResponseEntity<Void> putFailRule(@RequestBody RuleRequest request) {
        if (request == null || request.idempotencyKey() == null || request.failTimes() == null) {
            throw new IllegalArgumentException("idempotencyKey 与 failTimes 不能为空");
        }
        SimulatedDeliveryHandler.putFailRule(
                request.idempotencyKey(), request.failTimes(), Boolean.TRUE.equals(request.permanent()));
        return ResponseEntity.ok().build();
    }

    /** 测试/本地验证用：放行某幂等键的阻塞点。 */
    @PostMapping("/admin/blocks/release")
    public ResponseEntity<Void> releaseBlock(@RequestBody RuleRequest request) {
        if (request == null || request.idempotencyKey() == null) {
            throw new IllegalArgumentException("idempotencyKey 不能为空");
        }
        SimulatedDeliveryHandler.releaseBlock(request.idempotencyKey());
        return ResponseEntity.ok().build();
    }

    /** 参数非法统一返回 400。 */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }
}
