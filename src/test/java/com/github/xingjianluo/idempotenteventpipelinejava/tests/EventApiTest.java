package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * REST API 端到端：幂等提交、状态查询与统计。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventApiTest {

    private static final Logger log = LoggerFactory.getLogger(EventApiTest.class);

    @DynamicPropertySource
    static void pipelineProps(DynamicPropertyRegistry registry) throws Exception {
        Path store = Files.createTempDirectory("pipeline-api-test")
                .resolve("events-" + UUID.randomUUID() + ".jsonl");
        registry.add("pipeline.store-path", store::toString);
        registry.add("pipeline.poll-interval-ms", () -> "10");
        registry.add("pipeline.backoff-base-ms", () -> "20");
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    void submitQueryAndStats() {
        Map<String, String> request = Map.of(
                "idempotencyKey", "api-key-1",
                "orderKey", "order-1",
                "payload", "hello");

        ResponseEntity<Map> first = rest.postForEntity("/api/events", request, Map.class);
        ResponseEntity<Map> second = rest.postForEntity("/api/events", request, Map.class);

        log.info("幂等提交: idempotencyKey=api-key-1 首次={}/{} 重复={}/{}",
                first.getStatusCode(), first.getBody().get("outcome"),
                second.getStatusCode(), second.getBody().get("outcome"));
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(first.getBody().get("outcome")).isEqualTo("ACCEPTED");
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("outcome")).isEqualTo("DUPLICATE");
        assertThat(second.getBody().get("eventId")).isEqualTo(first.getBody().get("eventId"));

        String eventId = (String) first.getBody().get("eventId");
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            Map event = rest.getForObject("/api/events/" + eventId, Map.class);
            return EventStatus.DELIVERED.name().equals(event.get("status"));
        });
        Map event = rest.getForObject("/api/events/" + eventId, Map.class);
        log.info("单条状态查询: id={} status={} attempts={}", eventId, event.get("status"), event.get("attempts"));
        assertThat(event.get("idempotencyKey")).isEqualTo("api-key-1");

        Map byKey = rest.getForObject("/api/events/by-key/api-key-1", Map.class);
        assertThat(byKey.get("id")).isEqualTo(eventId);

        Map<String, Number> stats = rest.getForObject("/api/stats", Map.class);
        log.info("整体统计: {}", stats);
        assertThat(stats.get("DELIVERED").longValue()).isEqualTo(1L);
    }

    @Test
    void failingEventEndsUpInDeadLetterApi() {
        Map<String, String> request = Map.of(
                "idempotencyKey", "api-key-fail",
                "orderKey", "order-9",
                "payload", "FAIL please");
        rest.postForEntity("/api/events", request, Map.class);

        await().atMost(Duration.ofSeconds(10)).until(() -> {
            Map event = rest.getForObject("/api/events/by-key/api-key-fail", Map.class);
            return EventStatus.DEAD_LETTER.name().equals(event.get("status"));
        });
        var deadLetters = rest.getForObject("/api/dead-letters", java.util.List.class);
        Map event = rest.getForObject("/api/events/by-key/api-key-fail", Map.class);
        log.info("死信查询: idempotencyKey={} attempts={} lastError={}",
                event.get("idempotencyKey"), event.get("attempts"), event.get("lastError"));
        assertThat(deadLetters).hasSize(1);
        assertThat((String) event.get("lastError")).contains("模拟消费失败");
    }
}
