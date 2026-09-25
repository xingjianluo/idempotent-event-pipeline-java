package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.pipeline.EventConsumer;
import com.github.xingjianluo.idempotenteventpipelinejava.pipeline.EventDispatcher;
import com.github.xingjianluo.idempotenteventpipelinejava.service.EventService;
import com.github.xingjianluo.idempotenteventpipelinejava.store.FileEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 投递链路行为：退避重试、死信、顺序键隔离与重启恢复。
 */
class EventDispatcherTest {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcherTest.class);

    @TempDir
    Path dir;

    private PipelineProperties fastProperties() {
        PipelineProperties props = new PipelineProperties();
        props.setMaxAttempts(3);
        props.setBackoffBaseMs(20);
        props.setBackoffMaxMs(100);
        props.setPollIntervalMs(10);
        return props;
    }

    @Test
    void failedEventIsRetriedWithBackoffAndEventuallyDelivered() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        EventService service = new EventService(store, props);

        AtomicInteger failuresLeft = new AtomicInteger(2);
        ConcurrentLinkedQueue<Integer> attemptsSeen = new ConcurrentLinkedQueue<>();
        EventConsumer consumer = event -> {
            attemptsSeen.add(event.getAttempts());
            if (failuresLeft.getAndDecrement() > 0) {
                throw new IllegalStateException("flaky failure #" + event.getAttempts());
            }
        };
        EventDispatcher dispatcher = new EventDispatcher(store, consumer, props);
        dispatcher.start();
        try {
            service.submit("flaky-key", "order-1", "flaky");
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store.findByIdempotencyKey("flaky-key").orElseThrow().getStatus()
                            == EventStatus.DELIVERED);

            EventRecord done = store.findByIdempotencyKey("flaky-key").orElseThrow();
            log.info("重试后成功: idempotencyKey={} attempts={} status={} 消费端看到的尝试序号={}",
                    done.getIdempotencyKey(), done.getAttempts(), done.getStatus(), attemptsSeen);
            assertThat(done.getAttempts()).isEqualTo(3);
            assertThat(attemptsSeen).containsExactly(1, 2, 3);
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    void eventExceedingMaxAttemptsGoesToDeadLetterWithLastError() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        EventService service = new EventService(store, props);

        EventDispatcher dispatcher = new EventDispatcher(store, event -> {
            throw new IllegalArgumentException("always fails");
        }, props);
        dispatcher.start();
        try {
            service.submit("doomed-key", "order-1", "doomed");
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store.findByIdempotencyKey("doomed-key").orElseThrow().getStatus()
                            == EventStatus.DEAD_LETTER);

            EventRecord dead = store.findByIdempotencyKey("doomed-key").orElseThrow();
            log.info("进入死信: idempotencyKey={} attempts={} status={} lastError={}",
                    dead.getIdempotencyKey(), dead.getAttempts(), dead.getStatus(), dead.getLastError());
            assertThat(dead.getAttempts()).isEqualTo(props.getMaxAttempts());
            assertThat(dead.getLastError()).contains("always fails");
            assertThat(store.findDeadLetters()).hasSize(1);
            // 死信是终态：不再被调度
            assertThat(store.findDue(java.time.Instant.now().plusSeconds(60), 10)).isEmpty();
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    void sameOrderKeyIsSerialAndDifferentKeysDoNotBlock() throws Exception {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        EventService service = new EventService(store, props);

        CountDownLatch blockA1 = new CountDownLatch(1);
        CountDownLatch a1Started = new CountDownLatch(1);
        List<String> consumedOrder = new CopyOnWriteArrayList<>();
        EventConsumer consumer = event -> {
            consumedOrder.add(event.getPayload());
            if ("A1".equals(event.getPayload())) {
                a1Started.countDown();
                try {
                    blockA1.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        EventDispatcher dispatcher = new EventDispatcher(store, consumer, props);
        dispatcher.start();
        try {
            service.submit("key-a1", "order-A", "A1");
            service.submit("key-a2", "order-A", "A2");
            service.submit("key-a3", "order-A", "A3");
            service.submit("key-b1", "order-B", "B1");

            assertThat(a1Started.await(5, TimeUnit.SECONDS)).isTrue();
            // A1 阻塞期间，不同顺序键的 B1 必须能完成
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store.findByIdempotencyKey("key-b1").orElseThrow().getStatus()
                            == EventStatus.DELIVERED);
            log.info("A1 阻塞时 B1 已完成，顺序键互不阻塞: consumed={}", consumedOrder);
            // 同键的 A2/A3 不得越过 A1
            assertThat(consumedOrder).doesNotContain("A2", "A3");

            blockA1.countDown();
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store.countByStatus().get(EventStatus.DELIVERED) == 4L);
            List<String> aOrder = consumedOrder.stream().filter(p -> p.startsWith("A")).toList();
            log.info("同键事件保持相对顺序: {}", aOrder);
            assertThat(aOrder).containsExactly("A1", "A2", "A3");
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    void restartRecoversAndContinuesUnfinishedEvents() {
        Path file = dir.resolve("events.jsonl");
        PipelineProperties props = fastProperties();

        // 第一次运行：一条已完成，一条被认领后“进程崩溃”（IN_FLIGHT 残留），一条未开始
        FileEventStore store1 = new FileEventStore(file);
        EventService service1 = new EventService(store1, props);
        service1.submit("key-done", "order-1", "done");
        service1.submit("key-stuck", "order-1", "stuck");
        service1.submit("key-fresh", "order-2", "fresh");
        EventRecord done = store1.findByIdempotencyKey("key-done").orElseThrow();
        store1.claim(done.getId(), done.getVersion());
        store1.markDelivered(done.getId(), done.getVersion());
        EventRecord stuck = store1.findByIdempotencyKey("key-stuck").orElseThrow();
        store1.claim(stuck.getId(), stuck.getVersion());
        log.info("模拟崩溃: idempotencyKey={} status={}", stuck.getIdempotencyKey(), stuck.getStatus());

        // 重启：新存储实例 + 新调度器
        FileEventStore store2 = new FileEventStore(file);
        ConcurrentLinkedQueue<String> consumed = new ConcurrentLinkedQueue<>();
        EventDispatcher dispatcher = new EventDispatcher(store2,
                event -> consumed.add(event.getIdempotencyKey()), props);
        dispatcher.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store2.countByStatus().get(EventStatus.DELIVERED) == 3L);
            EventRecord recovered = store2.findByIdempotencyKey("key-stuck").orElseThrow();
            log.info("重启后恢复完成: idempotencyKey={} attempts={} status={} consumed={}",
                    recovered.getIdempotencyKey(), recovered.getAttempts(),
                    recovered.getStatus(), consumed);
            // 处理中的事件被恢复并完成，未开始的事件正常投递，已完成的不重复消费
            assertThat(consumed).containsExactlyInAnyOrder("key-stuck", "key-fresh");
            assertThat(store2.countByStatus().get(EventStatus.PENDING)).isZero();
            assertThat(store2.countByStatus().get(EventStatus.IN_FLIGHT)).isZero();
        } finally {
            dispatcher.stop();
        }
    }
}
