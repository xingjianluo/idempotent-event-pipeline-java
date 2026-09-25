package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 通过真实 HTTP 端口并发提交同一幂等键：
 * 恰好一个 201 ACCEPTED，其余为 409 REJECTED_CONCURRENT。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ConcurrentSubmitHttpTest extends AbstractPipelineIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmitHttpTest.class);

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void concurrentHttpSubmits_sameKey_onlyOneAccepted() throws Exception {
        String key = "http-concurrent-key-1";
        int threads = 6;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger created = new AtomicInteger();
        AtomicInteger conflict = new AtomicInteger();
        Map<Integer, String> outcomes = new java.util.concurrent.ConcurrentHashMap<>();

        for (int i = 0; i < threads; i++) {
            int idx = i;
            pool.submit(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    String body = "{\"idempotencyKey\":\"" + key + "\",\"orderKey\":\"K\",\"payload\":\"p" + idx + "\"}";
                    ResponseEntity<Map> resp = restTemplate.postForEntity(
                            "http://localhost:" + port + "/api/events",
                            new HttpEntity<>(body, headers), Map.class);
                    Object outcome = resp.getBody() == null ? "null" : resp.getBody().get("outcome");
                    outcomes.put(idx, resp.getStatusCode() + "/" + outcome);
                    log.info("并发提交 idempotencyKey={} 线程#{} HTTP={} outcome={}",
                            key, idx, resp.getStatusCode(), outcome);
                    if (resp.getStatusCode() == HttpStatus.CREATED) {
                        created.incrementAndGet();
                    } else if (resp.getStatusCode() == HttpStatus.CONFLICT) {
                        conflict.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        log.info("idempotencyKey={} HTTP 并发结果 201={} 409={} 明细={}",
                key, created.get(), conflict.get(), outcomes);
        assertEquals(1, created.get(), "只能有一个请求被接受");
        assertEquals(threads - 1, conflict.get(), "其余并发请求必须得到可区分的拒绝");

        @SuppressWarnings("unchecked")
        Map<String, Object> view = restTemplate.getForObject(
                "http://localhost:" + port + "/api/events/" + key, Map.class);
        assertNotNull(view);
        log.info("idempotencyKey={} 最终存储 status={} attempt={} payload={}",
                key, view.get("status"), view.get("attempt"), view.get("payload"));
        assertEquals("p" + findAcceptedIndex(outcomes), view.get("payload"),
                "生效请求的载荷必须是被接受的那一个");
    }

    private int findAcceptedIndex(Map<Integer, String> outcomes) {
        return outcomes.entrySet().stream()
                .filter(e -> e.getValue().startsWith("201"))
                .map(Map.Entry::getKey).findFirst().orElseThrow();
    }
}
