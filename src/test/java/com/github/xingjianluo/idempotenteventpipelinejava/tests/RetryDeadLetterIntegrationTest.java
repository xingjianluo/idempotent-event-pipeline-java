package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.handler.SimulatedDeliveryHandler;
import com.github.xingjianluo.idempotenteventpipelinejava.handler.TransientDeliveryException;
import com.github.xingjianluo.idempotenteventpipelinejava.handler.PermanentDeliveryException;
import com.github.xingjianluo.idempotenteventpipelinejava.service.EventService;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端：临时失败按退避重试后成功；失败次数耗尽进入死信并保留可区分的失败原因。
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RetryDeadLetterIntegrationTest extends AbstractPipelineIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RetryDeadLetterIntegrationTest.class);

    @Autowired
    private EventService eventService;

    @Autowired
    private EventStore store;

    @Test
    void transientFailure_retriedWithBackoff_thenDelivered() {
        String key = "e2e-retry-ok";
        SimulatedDeliveryHandler.putFailRule(key, 2, false);
        eventService.submit(key, "order-retry", "payload");

        EventRecord done = awaitStatus(key, EventStatus.DELIVERED, 5_000);
        log.info("idempotencyKey={} 最终状态={} attempt={}", key, done.getStatus(), done.getAttempt());
        assertEquals(3, done.getAttempt(), "前两次失败、第三次成功，尝试次数应为 3");
        assertEquals(3, SimulatedDeliveryHandler.invokedTimes(key));
        assertNotNull(done.getLastFailureReason());
        assertTrue(done.getLastFailureReason().contains(TransientDeliveryException.class.getSimpleName()),
                "失败原因必须可区分，记录具体异常类型");
    }

    @Test
    void permanentFailure_exhaustingAttempts_entersDeadLetterWithReason() {
        String key = "e2e-dead";
        SimulatedDeliveryHandler.putFailRule(key, 10, true);
        eventService.submit(key, "order-dlq", "payload");

        EventRecord dead = awaitStatus(key, EventStatus.DEAD_LETTER, 5_000);
        log.info("idempotencyKey={} 终态={} attempt={} 最后失败原因={}",
                key, dead.getStatus(), dead.getAttempt(), dead.getLastFailureReason());
        assertEquals(3, dead.getAttempt());
        assertEquals(3, SimulatedDeliveryHandler.invokedTimes(key));
        assertNotNull(dead.getLastFailureReason());
        assertTrue(dead.getLastFailureReason().contains(PermanentDeliveryException.class.getSimpleName()),
                "死信记录的失败原因应能区分永久性错误");

        // 死信是终态：重复提交得到复用结论，且不再触发投递
        EventStore.SubmitResult again = eventService.submit(key, "order-dlq", "payload");
        assertEquals(com.github.xingjianluo.idempotenteventpipelinejava.domain.SubmitOutcome.REUSED_DEAD_LETTER,
                again.getOutcome());
        log.info("idempotencyKey={} 死信后重复提交结论={}", key, again.getOutcome());
        assertEquals(3, SimulatedDeliveryHandler.invokedTimes(key), "死信事件不得再次投递");
    }

    private EventRecord awaitStatus(String key, EventStatus expected, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        EventRecord e;
        while (System.currentTimeMillis() < deadline) {
            e = store.findByKey(key);
            if (e != null && e.getStatus() == expected) {
                return e;
            }
            try {
                Thread.sleep(30);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
        e = store.findByKey(key);
        throw new AssertionError("idempotencyKey=" + key + " 在 " + timeoutMs + "ms 内未达到 "
                + expected + "，当前=" + (e == null ? "不存在" : e.getStatus()
                + " attempt=" + e.getAttempt() + " reason=" + e.getLastFailureReason()));
    }
}
