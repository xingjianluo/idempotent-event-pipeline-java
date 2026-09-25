package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.handler.SimulatedDeliveryHandler;
import com.github.xingjianluo.idempotenteventpipelinejava.service.EventService;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 顺序语义：相同顺序键严格 FIFO；不同顺序键并行、互不阻塞。
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrderingIntegrationTest extends AbstractPipelineIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OrderingIntegrationTest.class);

    @Autowired
    private EventService eventService;

    @Autowired
    private EventStore store;

    @Test
    void sameOrderKey_fifo_differentOrderKeysNotBlocked() throws Exception {
        // 阻塞 same 键的第 1 条事件（fifo-1 在 attempt=1 处挂住）
        SimulatedDeliveryHandler.BlockHandle block = SimulatedDeliveryHandler.armBlock("fifo-1", 1);

        eventService.submit("fifo-1", "same", "1");
        eventService.submit("fifo-2", "same", "2");
        eventService.submit("other-1", "other", "x");

        // 等待 fifo-1 真正进入下游并挂在阻塞点上，避免与轮询存在竞态
        assertTrue(block.entered().await(5, java.util.concurrent.TimeUnit.SECONDS),
                "fifo-1 应已进入下游处理");

        // other 键不被 same 键阻塞，应很快完成
        EventAwait.await(store, "other-1", EventStatus.DELIVERED, Duration.ofSeconds(5));
        log.info("idempotencyKey=other-1 已在不同顺序键上独立完成，未被 same 键阻塞");

        // 此时 same 键的第 2 条必须仍在等待队首
        assertEquals(0, SimulatedDeliveryHandler.invokedTimes("fifo-2"),
                "同键后续事件在队首未完成前不得开始投递");
        log.info("队首 fifo-1 阻塞期间 fifo-2 尚未投递，满足 FIFO");

        // 放行队首，之后两条按序完成
        block.release().countDown();
        EventAwait.await(store, "fifo-1", EventStatus.DELIVERED, Duration.ofSeconds(5));
        EventAwait.await(store, "fifo-2", EventStatus.DELIVERED, Duration.ofSeconds(5));

        long t1 = store.findByKey("fifo-1").getUpdatedAt().toEpochMilli();
        long t2 = store.findByKey("fifo-2").getUpdatedAt().toEpochMilli();
        assertTrue(t1 <= t2, "同键事件完成顺序必须与提交顺序一致");
        log.info("idempotencyKey=fifo-1/fifo-2 顺序完成时间戳 {} <= {}", t1, t2);

        assertEquals(1, SimulatedDeliveryHandler.invokedTimes("fifo-1"));
        assertEquals(1, SimulatedDeliveryHandler.invokedTimes("fifo-2"));
    }
}
