package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.Application;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.handler.SimulatedDeliveryHandler;
import com.github.xingjianluo.idempotenteventpipelinejava.pipeline.PipelineWorker;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 进程重启恢复：
 * <ol>
 *   <li>第一段进程：一条事件持续失败（阻塞于退避中 PENDING），另一条已成功；</li>
 *   <li>关闭上下文模拟进程退出；</li>
 *   <li>第二段进程指向同一数据目录：未完成事件继续处理直至成功；已成功的事件不重复投递。</li>
 * </ol>
 * 注意：模拟下游的失败规则保存在进程内存中，因此第二段进程默认放行成功，
 * 正好模拟“下游从临时故障中恢复”。
 */
class RestartRecoveryIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryIntegrationTest.class);

    private Path tempDir;
    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        SimulatedDeliveryHandler.clearTestState();
        tempDir = Files.createTempDirectory("pipeline-restart-" + System.nanoTime() + "-");
        log.info("本用例独立数据目录: {}", tempDir);
    }

    @AfterEach
    void closeAll() {
        for (ConfigurableApplicationContext c : contexts) {
            try {
                c.close();
            } catch (Exception ignored) {
                // 忽略关闭异常
            }
        }
        contexts.clear();
    }

    private ConfigurableApplicationContext startContext() {
        // 命令行参数优先级高于打包的 application.properties，确保使用本次测试的临时目录。
        // 显式关闭 JVM shutdown hook：否则第一个上下文注册的 hook 在后续关闭时会连带影响测试 JVM。
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Application.class)
                .registerShutdownHook(false)
                .run(
                        "--server.port=0",
                        "--pipeline.data-dir=" + tempDir.toString(),
                        "--pipeline.poll-interval-ms=30",
                        "--pipeline.max-attempts=10",
                        "--pipeline.backoff-initial-ms=60000",
                        "--pipeline.backoff-max-ms=60000");
        contexts.add(ctx);
        PipelineWorker worker = ctx.getBean(PipelineWorker.class);
        assertTrue(worker.isRunning(), "worker 必须随上下文启动");
        return ctx;
    }

    @Test
    void unfinishedEventContinuedAfterRestart_deliveredEventNotRepeated() {
        // 第一段进程
        ConfigurableApplicationContext first = startContext();
        EventStore store1 = first.getBean(EventStore.class);

        // fail-forever：第一段进程内一直失败；长退避保证第一次失败后长期处于 PENDING 等待重试
        SimulatedDeliveryHandler.putFailRule("will-recover", 1_000_000, false);
        var svc1 = first.getBean(com.github.xingjianluo.idempotenteventpipelinejava.service.EventService.class);
        svc1.submit("will-recover", "order-R", "v");
        svc1.submit("already-done", "order-D", "v");

        // 等待第一次失败发生（此时因 60s 退避稳定停留在 PENDING）
        waitForAttemptAtLeast(store1, "will-recover", 1);
        waitForStatus(store1, "will-recover", EventStatus.PENDING, Duration.ofSeconds(3));
        EventRecord failed = store1.findByKey("will-recover");
        int attemptsBeforeRestart = failed.getAttempt();
        log.info("重启前 idempotencyKey=will-recover status={} attempt={} 原因={}",
                failed.getStatus(), attemptsBeforeRestart, failed.getLastFailureReason());
        assertEquals(1, attemptsBeforeRestart);
        EventAwait.await(store1, "already-done", EventStatus.DELIVERED, Duration.ofSeconds(5));
        assertEquals(1, SimulatedDeliveryHandler.invokedTimes("already-done"));

        // 模拟进程退出
        first.close();
        contexts.remove(first);
        log.info("第一段进程已关闭，存储目录保持不变: {}", tempDir);

        // 第二段进程：模拟下游恢复（清空内存中的失败规则）
        SimulatedDeliveryHandler.clearTestState();
        ConfigurableApplicationContext second = startContext();
        EventStore store2 = second.getBean(EventStore.class);

        EventRecord recovered = EventAwait.await(
                store2, "will-recover", EventStatus.DELIVERED, Duration.ofSeconds(10));
        log.info("重启后 idempotencyKey=will-recover 最终 status={} attempt={}",
                recovered.getStatus(), recovered.getAttempt());
        assertNotNull(recovered.getLastFailureReason(), "历史失败原因应保留，语义可解释");

        EventRecord done = store2.findByKey("already-done");
        assertEquals(EventStatus.DELIVERED, done.getStatus());
        assertEquals(1, done.getAttempt(), "已完成事件重启后不得重复投递");
        log.info("重启后 idempotencyKey=already-done 仍为 DELIVERED attempt=1，未重复完成");
    }

    @Test
    void processingEventInterruptedByKill_isRecoveredAndRedeliveredOnce() throws Exception {
        ConfigurableApplicationContext first = startContext();
        EventStore store1 = first.getBean(EventStore.class);
        var svc1 = first.getBean(com.github.xingjianluo.idempotenteventpipelinejava.service.EventService.class);

        // 让事件在第一次投递时一直挂住，模拟“正在投递时进程被 kill -9”
        SimulatedDeliveryHandler.BlockHandle block = SimulatedDeliveryHandler.armBlock("inflight", 1);
        svc1.submit("inflight", "order-P", "v");

        assertTrue(block.entered().await(5, TimeUnit.SECONDS), "事件应已进入下游处理");
        EventRecord inflight = store1.findByKey("inflight");
        assertEquals(EventStatus.PROCESSING, inflight.getStatus());
        log.info("重启前 idempotencyKey=inflight status=PROCESSING attempt={} lease={}",
                inflight.getAttempt(), inflight.getLeaseOwner());

        // 强杀式关闭：不等待投递线程结束（与 kill -9 等价的语义），下游仍阻塞在 release 上
        first.close();
        contexts.remove(first);
        SimulatedDeliveryHandler.clearTestState();

        ConfigurableApplicationContext second = startContext();
        EventStore store2 = second.getBean(EventStore.class);

        EventRecord redelivered = EventAwait.await(
                store2, "inflight", EventStatus.DELIVERED, Duration.ofSeconds(10));
        log.info("重启后 idempotencyKey=inflight status={} attempt={}（崩溃在途的事件被重新投递）",
                redelivered.getStatus(), redelivered.getAttempt());
        assertEquals(2, redelivered.getAttempt(),
                "崩溃在途的事件恢复后应再投递一次（at-least-once），尝试次数继续累加");
    }

    private void waitForStatus(EventStore store, String key, EventStatus expected, Duration timeout) {
        EventAwait.await(store, key, expected, timeout);
    }

    private void waitForAttemptAtLeast(EventStore store, String key, int min) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            EventRecord e = store.findByKey(key);
            if (e != null && e.getAttempt() >= min) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
        throw new AssertionError("idempotencyKey=" + key + " attempt 未达到 " + min);
    }
}
