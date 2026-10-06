package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.model.BatchReplayResult;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.ReplayOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.ReplayResult;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 死信重投：幂等重投、配额上限、批量上限、顺序保持与重启恢复。
 */
class DeadLetterReplayTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayTest.class);

    @TempDir
    Path dir;

    private PipelineProperties fastProperties() {
        PipelineProperties props = new PipelineProperties();
        props.setMaxAttempts(1); // 一次失败即进死信，便于快速构造死信
        props.setBackoffBaseMs(20);
        props.setBackoffMaxMs(100);
        props.setPollIntervalMs(10);
        props.setMaxReplays(3);
        props.setReplayBatchMaxLimit(100);
        return props;
    }

    private EventRecord awaitStatus(FileEventStore store, String key, EventStatus status) {
        await().atMost(Duration.ofSeconds(5)).until(() ->
                store.findByIdempotencyKey(key).orElseThrow().getStatus() == status);
        return store.findByIdempotencyKey(key).orElseThrow();
    }

    @Test
    void concurrentDuplicateReplayTakesEffectOnlyOnce() throws Exception {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        EventService service = new EventService(store, props);

        EventDispatcher dispatcher = new EventDispatcher(store, event -> {
            throw new IllegalStateException("always fails");
        }, props);
        dispatcher.start();
        service.submit("replay-race", "order-1", "doomed");
        EventRecord dead = awaitStatus(store, "replay-race", EventStatus.DEAD_LETTER);
        dispatcher.stop();

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        ConcurrentLinkedQueue<ReplayResult> results = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    gate.await();
                    results.add(service.replay(dead.getId()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        gate.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        long replayed = results.stream().filter(ReplayResult::replayed).count();
        long rejected = results.stream()
                .filter(r -> r.outcome() == ReplayOutcome.NOT_DEAD_LETTER).count();
        EventRecord after = store.findById(dead.getId()).orElseThrow();
        log.info("并发重投: idempotencyKey={} 生效={} 拒绝={} replayCount={} remaining={} status={}",
                after.getIdempotencyKey(), replayed, rejected, after.getReplayCount(),
                after.getRemainingReplays(), after.getStatus());
        assertThat(results).hasSize(threads);
        assertThat(replayed).isEqualTo(1);
        assertThat(rejected).isEqualTo(threads - 1);
        assertThat(after.getReplayCount()).isEqualTo(1);
        assertThat(after.getLastReplayAt()).isNotNull();
        assertThat(after.getRemainingReplays()).isEqualTo(props.getMaxReplays() - 1);
        assertThat(after.getStatus()).isEqualTo(EventStatus.PENDING);
    }

    @Test
    void replayQuotaExceededIsRejectedAndReasonSurvivesRestart() {
        Path file = dir.resolve("events.jsonl");
        PipelineProperties props = fastProperties();
        props.setMaxReplays(1);
        FileEventStore store = new FileEventStore(file);
        EventService service = new EventService(store, props);

        EventDispatcher dispatcher = new EventDispatcher(store, event -> {
            throw new IllegalStateException("always fails");
        }, props);
        dispatcher.start();
        try {
            service.submit("quota-key", "order-1", "doomed");
            EventRecord dead = awaitStatus(store, "quota-key", EventStatus.DEAD_LETTER);

            ReplayResult first = service.replay(dead.getId());
            assertThat(first.outcome()).isEqualTo(ReplayOutcome.REPLAYED);
            // 重投后再次失败，重新进入死信
            EventRecord deadAgain = awaitStatus(store, "quota-key", EventStatus.DEAD_LETTER);
            assertThat(deadAgain.getReplayCount()).isEqualTo(1);

            ReplayResult second = service.replay(dead.getId());
            log.info("超限重投被拒: idempotencyKey={} outcome={} reason={}",
                    "quota-key", second.outcome(), second.reason());
            assertThat(second.outcome()).isEqualTo(ReplayOutcome.QUOTA_EXCEEDED);
            assertThat(second.reason()).contains("上限");
            assertThat(store.findById(dead.getId()).orElseThrow().getStatus())
                    .isEqualTo(EventStatus.DEAD_LETTER);
        } finally {
            dispatcher.stop();
        }

        // 重启后：已用掉的重投次数与拒绝原因不丢
        FileEventStore reloaded = new FileEventStore(file);
        EventRecord persisted = reloaded.findByIdempotencyKey("quota-key").orElseThrow();
        log.info("重启后重投状态: idempotencyKey={} replayCount={} lastReplayAt={} rejectReason={}",
                persisted.getIdempotencyKey(), persisted.getReplayCount(),
                persisted.getLastReplayAt(), persisted.getLastReplayRejectReason());
        assertThat(persisted.getReplayCount()).isEqualTo(1);
        assertThat(persisted.getLastReplayAt()).isNotNull();
        assertThat(persisted.getLastReplayRejectReason()).contains("上限");
        assertThat(reloaded.replayStats().get("totalReplays")).isEqualTo(1L);
        assertThat(reloaded.replayStats().get("eventsReplayed")).isEqualTo(1L);
    }

    @Test
    void nonDeadLetterReplayIsRejectedWithReason() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        EventService service = new EventService(store, props);

        // 正常处理中（PENDING）的事件：拒绝
        service.submit("pending-key", "order-1", "hello");
        EventRecord pending = store.findByIdempotencyKey("pending-key").orElseThrow();
        ReplayResult pendingReplay = service.replay(pending.getId());
        log.info("非死信重投被拒: idempotencyKey={} status={} outcome={} reason={}",
                "pending-key", pending.getStatus(), pendingReplay.outcome(), pendingReplay.reason());
        assertThat(pendingReplay.outcome()).isEqualTo(ReplayOutcome.NOT_DEAD_LETTER);
        assertThat(pendingReplay.reason()).contains("PENDING");

        // 已投递成功的事件：拒绝
        EventDispatcher dispatcher = new EventDispatcher(store, event -> {
        }, props);
        dispatcher.start();
        try {
            awaitStatus(store, "pending-key", EventStatus.DELIVERED);
            ReplayResult deliveredReplay = service.replay(pending.getId());
            log.info("已成功事件重投被拒: idempotencyKey={} outcome={} reason={}",
                    "pending-key", deliveredReplay.outcome(), deliveredReplay.reason());
            assertThat(deliveredReplay.outcome()).isEqualTo(ReplayOutcome.NOT_DEAD_LETTER);
            assertThat(deliveredReplay.reason()).contains("DELIVERED");

            // 不存在的事件：NOT_FOUND
            ReplayResult missing = service.replay("no-such-id");
            assertThat(missing.outcome()).isEqualTo(ReplayOutcome.NOT_FOUND);
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    void batchReplayRespectsLimitAndConfigCap() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        props.setReplayBatchMaxLimit(2); // 配置上限为 2
        EventService service = new EventService(store, props);

        EventDispatcher dispatcher = new EventDispatcher(store, event -> {
            throw new IllegalStateException("always fails");
        }, props);
        dispatcher.start();
        for (int i = 0; i < 5; i++) {
            service.submit("batch-key-" + i, "order-" + i, "doomed-" + i);
        }
        await().atMost(Duration.ofSeconds(5)).until(() ->
                store.countByStatus().get(EventStatus.DEAD_LETTER) == 5L);
        dispatcher.stop();

        // 请求 100 条，被配置上限截断为 2
        BatchReplayResult batch = service.replayBatch(100);
        log.info("批量重投: limit={} replayed={} rejected={} 剩余死信={}",
                batch.limit(), batch.replayed(), batch.rejected(),
                store.countByStatus().get(EventStatus.DEAD_LETTER));
        assertThat(batch.limit()).isEqualTo(2);
        assertThat(batch.replayed()).isEqualTo(2);
        assertThat(store.countByStatus().get(EventStatus.DEAD_LETTER)).isEqualTo(3L);
        // 按提交先后，最早的两条被重投
        assertThat(store.findByIdempotencyKey("batch-key-0").orElseThrow().getStatus())
                .isEqualTo(EventStatus.PENDING);
        assertThat(store.findByIdempotencyKey("batch-key-1").orElseThrow().getStatus())
                .isEqualTo(EventStatus.PENDING);
        assertThat(store.findByIdempotencyKey("batch-key-2").orElseThrow().getStatus())
                .isEqualTo(EventStatus.DEAD_LETTER);
    }

    @Test
    void earlierEventWaitingRetryBlocksLaterEventOfSameKey() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        props.setMaxAttempts(3);
        props.setBackoffBaseMs(400); // 前一条失败后长时间退避等待
        props.setBackoffMaxMs(400);
        EventService service = new EventService(store, props);

        List<String> consumedOrder = new CopyOnWriteArrayList<>();
        AtomicInteger aAttempts = new AtomicInteger();
        EventConsumer consumer = event -> {
            consumedOrder.add(event.getPayload());
            if ("A".equals(event.getPayload()) && aAttempts.incrementAndGet() == 1) {
                throw new IllegalStateException("A first attempt fails");
            }
        };
        EventDispatcher dispatcher = new EventDispatcher(store, consumer, props);
        dispatcher.start();
        try {
            service.submit("head-key", "order-K", "A");
            service.submit("tail-key", "order-K", "B");

            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store.countByStatus().get(EventStatus.DELIVERED) == 2L);
            EventRecord head = store.findByIdempotencyKey("head-key").orElseThrow();
            EventRecord tail = store.findByIdempotencyKey("tail-key").orElseThrow();
            log.info("队首阻塞: head(idempotencyKey={} attempts={}) 退避期间 tail(idempotencyKey={}) 未抢先, 消费顺序={}",
                    head.getIdempotencyKey(), head.getAttempts(), tail.getIdempotencyKey(), consumedOrder);
            // A 第一次失败后处于退避等待，B 不得越过；最终顺序必须是 A 在前
            assertThat(consumedOrder).containsExactly("A", "A", "B");
            assertThat(head.getAttempts()).isEqualTo(2);
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    void replayedEventKeepsOriginalPositionInOrderKey() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        EventService service = new EventService(store, props);

        // 第一轮：A 失败进死信后停掉调度器，再提交同键的 B（尚未投递）
        EventDispatcher dispatcher1 = new EventDispatcher(store, event -> {
            throw new IllegalStateException("always fails");
        }, props);
        dispatcher1.start();
        service.submit("pos-a", "order-P", "A");
        awaitStatus(store, "pos-a", EventStatus.DEAD_LETTER);
        dispatcher1.stop();

        service.submit("pos-b", "order-P", "B");
        ReplayResult replay = service.replay(
                store.findByIdempotencyKey("pos-a").orElseThrow().getId());
        assertThat(replay.replayed()).isTrue();

        // 第二轮：全部消费成功，重投的 A 必须按原位置排在 B 之前
        List<String> consumedOrder = new CopyOnWriteArrayList<>();
        EventDispatcher dispatcher2 = new EventDispatcher(store,
                event -> consumedOrder.add(event.getPayload()), props);
        dispatcher2.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store.countByStatus().get(EventStatus.DELIVERED) == 2L);
            log.info("重投保持原位: idempotencyKey=pos-a replayCount={} 消费顺序={}",
                    store.findByIdempotencyKey("pos-a").orElseThrow().getReplayCount(), consumedOrder);
            assertThat(consumedOrder).containsExactly("A", "B");
        } finally {
            dispatcher2.stop();
        }
    }

    @Test
    void replaySurvivesRestartAndDeliveredEventIsNotRedelivered() throws Exception {
        Path file = dir.resolve("events.jsonl");
        PipelineProperties props = fastProperties();
        props.setMaxReplays(2);

        // 第一轮：事件失败进死信
        FileEventStore store1 = new FileEventStore(file);
        EventService service1 = new EventService(store1, props);
        EventDispatcher dispatcher1 = new EventDispatcher(store1, event -> {
            throw new IllegalStateException("always fails");
        }, props);
        dispatcher1.start();
        service1.submit("restart-replay", "order-1", "doomed");
        EventRecord dead = awaitStatus(store1, "restart-replay", EventStatus.DEAD_LETTER);
        dispatcher1.stop();

        // 重投生效后“进程崩溃”（事件处于 PENDING，未投递）
        ReplayResult replay = service1.replay(dead.getId());
        assertThat(replay.replayed()).isTrue();
        log.info("重投后模拟崩溃: idempotencyKey={} replayCount={} status={}",
                "restart-replay", replay.event().getReplayCount(), replay.event().getStatus());

        // 重启：重投次数不丢，未完成的重投事件继续投递并成功
        FileEventStore store2 = new FileEventStore(file);
        EventRecord restored = store2.findByIdempotencyKey("restart-replay").orElseThrow();
        assertThat(restored.getReplayCount()).isEqualTo(1);
        assertThat(restored.getStatus()).isEqualTo(EventStatus.PENDING);

        ConcurrentLinkedQueue<String> consumed = new ConcurrentLinkedQueue<>();
        EventDispatcher dispatcher2 = new EventDispatcher(store2,
                event -> consumed.add(event.getIdempotencyKey()), props);
        dispatcher2.start();
        awaitStatus(store2, "restart-replay", EventStatus.DELIVERED);
        dispatcher2.stop();
        log.info("重启后重投事件完成: idempotencyKey={} replayCount={} consumed={}",
                "restart-replay",
                store2.findByIdempotencyKey("restart-replay").orElseThrow().getReplayCount(), consumed);
        assertThat(consumed).containsExactly("restart-replay");

        // 再次重启：已成功的事件不得被重复投递
        FileEventStore store3 = new FileEventStore(file);
        ConcurrentLinkedQueue<String> consumedAfter = new ConcurrentLinkedQueue<>();
        EventDispatcher dispatcher3 = new EventDispatcher(store3,
                event -> consumedAfter.add(event.getIdempotencyKey()), props);
        dispatcher3.start();
        Thread.sleep(300);
        dispatcher3.stop();
        assertThat(consumedAfter).isEmpty();
        assertThat(store3.findByIdempotencyKey("restart-replay").orElseThrow().getReplayCount())
                .isEqualTo(1);
    }
}
