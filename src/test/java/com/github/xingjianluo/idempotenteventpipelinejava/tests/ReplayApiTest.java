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
 * 死信重投 REST 端到端：单条重投、配额拒绝、批量重投与统计。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReplayApiTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayApiTest.class);

    @DynamicPropertySource
    static void pipelineProps(DynamicPropertyRegistry registry) throws Exception {
        Path store = Files.createTempDirectory("pipeline-replay-api-test")
                .resolve("events-" + UUID.randomUUID() + ".jsonl");
        registry.add("pipeline.store-path", store::toString);
        registry.add("pipeline.poll-interval-ms", () -> "10");
        registry.add("pipeline.backoff-base-ms", () -> "20");
        registry.add("pipeline.backoff-max-ms", () -> "50");
        registry.add("pipeline.max-replays", () -> "1");
        registry.add("pipeline.replay-batch-max-limit", () -> "2");
    }

    @Autowired
    TestRestTemplate rest;

    private String awaitStatus(String key, EventStatus status) {
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            Map event = rest.getForObject("/api/events/by-key/" + key, Map.class);
            return status.name().equals(event.get("status"));
        });
        return (String) rest.getForObject("/api/events/by-key/" + key, Map.class).get("id");
    }

    @Test
    void replayViaApiUntilQuotaExhausted() {
        rest.postForEntity("/api/events",
                Map.of("idempotencyKey", "api-replay-1", "orderKey", "order-r", "payload", "FAIL me"),
                Map.class);
        String id = awaitStatus("api-replay-1", EventStatus.DEAD_LETTER);

        // 第一次重投：生效（maxReplays=1）
        ResponseEntity<Map> first = rest.postForEntity(
                "/api/dead-letters/" + id + "/replay", null, Map.class);
        log.info("API 单条重投: idempotencyKey=api-replay-1 第一次={}/{} replayCount={}",
                first.getStatusCode(), first.getBody().get("outcome"), first.getBody().get("replayCount"));
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody().get("outcome")).isEqualTo("REPLAYED");
        assertThat(first.getBody().get("replayCount")).isEqualTo(1);
        assertThat(first.getBody().get("remainingReplays")).isEqualTo(0);

        // 重投后仍失败（payload 含 FAIL），再次进入死信；此时配额已用尽
        awaitStatus("api-replay-1", EventStatus.DEAD_LETTER);
        ResponseEntity<Map> second = rest.postForEntity(
                "/api/dead-letters/" + id + "/replay", null, Map.class);
        log.info("API 超限重投被拒: outcome={} reason={}",
                second.getBody().get("outcome"), second.getBody().get("reason"));
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(second.getBody().get("outcome")).isEqualTo("QUOTA_EXCEEDED");

        // 已成功事件重投：409 并说明原因
        rest.postForEntity("/api/events",
                Map.of("idempotencyKey", "api-replay-ok", "orderKey", "order-r", "payload", "fine"),
                Map.class);
        String okId = awaitStatus("api-replay-ok", EventStatus.DELIVERED);
        ResponseEntity<Map> deliveredReplay = rest.postForEntity(
                "/api/dead-letters/" + okId + "/replay", null, Map.class);
        assertThat(deliveredReplay.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(deliveredReplay.getBody().get("outcome")).isEqualTo("NOT_DEAD_LETTER");
        assertThat((String) deliveredReplay.getBody().get("reason")).contains("DELIVERED");

        // 不存在的事件：404
        ResponseEntity<Map> missing = rest.postForEntity(
                "/api/dead-letters/no-such-id/replay", null, Map.class);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // 批量重投：唯一一条死信已超限，replayed=0，rejected=1，limit 被配置截断
        Map batch = rest.postForEntity("/api/dead-letters/replay",
                Map.of("limit", 100), Map.class).getBody();
        log.info("API 批量重投: limit={} replayed={} rejected={}",
                batch.get("limit"), batch.get("replayed"), batch.get("rejected"));
        assertThat(batch.get("limit")).isEqualTo(2);
        assertThat(batch.get("replayed")).isEqualTo(0);
        assertThat(batch.get("rejected")).isEqualTo(1);

        // 统计包含死信与重投汇总
        Map<String, Number> stats = rest.getForObject("/api/stats", Map.class);
        log.info("API 统计: {}", stats);
        assertThat(stats.get("DEAD_LETTER").longValue()).isEqualTo(1L);
        assertThat(stats.get("DELIVERED").longValue()).isEqualTo(1L);
        assertThat(stats.get("totalReplays").longValue()).isEqualTo(1L);
        assertThat(stats.get("eventsReplayed").longValue()).isEqualTo(1L);
    }
}
