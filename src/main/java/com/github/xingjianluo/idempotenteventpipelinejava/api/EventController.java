package com.github.xingjianluo.idempotenteventpipelinejava.api;

import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitResult;
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
    public Map<EventStatus, Long> stats() {
        return service.stats();
    }

    @GetMapping("/dead-letters")
    public List<EventRecord> deadLetters() {
        return service.deadLetters();
    }
}
