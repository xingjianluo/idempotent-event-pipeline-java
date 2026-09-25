package com.github.xingjianluo.idempotenteventpipelinejava.web.dto;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;

import java.time.Instant;

/**
 * 单条事件的对外视图。
 */
public record EventView(
        long seq,
        String idempotencyKey,
        String orderKey,
        String payload,
        String status,
        int attempt,
        Instant createdAt,
        Instant updatedAt,
        Instant nextAttemptAt,
        String lastFailureReason) {

    public static EventView from(EventRecord e) {
        return new EventView(
                e.getSeq(),
                e.getIdempotencyKey(),
                e.getOrderKey(),
                e.getPayload(),
                e.getStatus().name(),
                e.getAttempt(),
                e.getCreatedAt(),
                e.getUpdatedAt(),
                e.getNextAttemptAt(),
                e.getLastFailureReason());
    }
}
