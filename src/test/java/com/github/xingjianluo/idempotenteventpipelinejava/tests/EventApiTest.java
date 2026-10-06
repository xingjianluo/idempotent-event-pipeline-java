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

        Map<String, Object> stats = rest.getForObject("/api/stats", Map.class);
        log.info("整体统计: {}", stats);
        Map<String, Number> byStatus = (Map<String, Number>) stats.get("byStatus");
        assertThat(byStatus.get("DELIVERED").longValue()).isEqualTo(1L);
        assertThat(((Number) stats.get("totalRequeues")).longValue()).isGreaterThanOrEqualTo(0L);
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
        log.info("死信查询: idempotencyKey={} attempts={} lastError={} 死信总数={}",
                event.get("idempotencyKey"), event.get("attempts"), event.get("lastError"),
                deadLetters.size());
        // 共享上下文中其他用例也会产生死信，这里确认目标事件在死信列表中
        assertThat(deadLetters).anySatisfy(e ->
                assertThat(((Map) e).get("idempotencyKey")).isEqualTo("api-key-fail"));
        assertThat((String) event.get("lastError")).contains("模拟消费失败");
    }

    @Test
    void deadLetterCanBeRequeuedViaApiAndDuplicateRequeueIsRejected() {
        Map<String, String> request = Map.of(
                "idempotencyKey", "api-key-requeue",
                "orderKey", "order-rq",
                "payload", "FAIL then requeue");
        String eventId = (String) rest.postForEntity("/api/events", request, Map.class)
                .getBody().get("eventId");

        await().atMost(Duration.ofSeconds(10)).until(() -> {
            Map event = rest.getForObject("/api/events/" + eventId, Map.class);
            return EventStatus.DEAD_LETTER.name().equals(event.get("status"));
        });

        // 单条重投生效
        ResponseEntity<Map> requeued = rest.postForEntity(
                "/api/dead-letters/" + eventId + "/requeue", null, Map.class);
        log.info("单条重投: eventId={} status={} body={}", eventId, requeued.getStatusCode(), requeued.getBody());
        assertThat(requeued.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(requeued.getBody().get("outcome")).isEqualTo("REQUEUED");
        assertThat(((Number) requeued.getBody().get("requeueCount")).intValue()).isEqualTo(1);

        // 已重新入队（非死信），重复重投被 409 拒绝且原因可区分
        ResponseEntity<Map> duplicate = rest.postForEntity(
                "/api/dead-letters/" + eventId + "/requeue", null, Map.class);
        log.info("重复重投被拒: status={} body={}", duplicate.getStatusCode(), duplicate.getBody());
        assertThat(duplicate.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(duplicate.getBody().get("outcome")).isEqualTo("NOT_DEAD_LETTER");

        // 单条查询可见重投次数与剩余配额
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            Map event = rest.getForObject("/api/events/" + eventId, Map.class);
            return ((Number) event.get("requeueCount")).intValue() == 1;
        });
        Map event = rest.getForObject("/api/events/" + eventId, Map.class);
        log.info("重投后单条查询: requeueCount={} lastRequeuedAt={} remainingRequeues={}",
                event.get("requeueCount"), event.get("lastRequeuedAt"), event.get("remainingRequeues"));
        assertThat(event.get("lastRequeuedAt")).isNotNull();

        // 统计中可见重投情况
        Map<String, Object> stats = rest.getForObject("/api/stats", Map.class);
        log.info("重投后统计: {}", stats);
        assertThat(((Number) stats.get("totalRequeues")).longValue()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    void batchRequeueViaApiRespectsLimit() {
        // 造两条死信（默认消费者对含 FAIL 的负载抛异常）
        for (int i = 0; i < 2; i++) {
            rest.postForEntity("/api/events", Map.of(
                    "idempotencyKey", "api-batch-" + i,
                    "orderKey", "order-batch-" + i,
                    "payload", "FAIL batch"), Map.class);
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            Map<String, Object> stats = rest.getForObject("/api/stats", Map.class);
            Map<String, Number> byStatus = (Map<String, Number>) stats.get("byStatus");
            return byStatus.get("DEAD_LETTER").longValue() >= 2L;
        });

        ResponseEntity<Map> batch = rest.postForEntity(
                "/api/dead-letters/requeue-batch", Map.of("limit", 100), Map.class);
        log.info("批量重投: status={} body={}", batch.getStatusCode(), batch.getBody());
        assertThat(batch.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) batch.getBody().get("requeued")).longValue()).isGreaterThanOrEqualTo(2L);
    }
}
