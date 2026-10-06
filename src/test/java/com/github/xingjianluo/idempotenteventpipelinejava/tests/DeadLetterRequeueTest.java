package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.RequeueOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.RequeueResult;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 死信重投：幂等重投、状态约束、配额上限与批量上限。
 */
class DeadLetterRequeueTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRequeueTest.class);

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

    /** 直接经存储层把一条新事件推进到死信，避免依赖调度时序。 */
    private EventRecord deadLetter(EventService service, FileEventStore store,
                                   String key, String orderKey) {
        service.submit(key, orderKey, "payload-of-" + key);
        EventRecord record = store.findByIdempotencyKey(key).orElseThrow();
        store.claim(record.getId(), record.getVersion());
        store.markDead(record.getId(), record.getVersion(), record.getMaxAttempts(),
                "simulated poison: " + key);
        return store.findById(record.getId()).orElseThrow();
    }

    @Test
    void concurrentRequeueOfSameDeadLetterTakesEffectExactlyOnce() throws Exception {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        EventService service = new EventService(store, fastProperties());
        EventRecord dead = deadLetter(service, store, "race-key", "order-race");

        int threads = 16;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ConcurrentLinkedQueue<RequeueResult> results = new ConcurrentLinkedQueue<>();
        List<Thread> workers = IntStream.range(0, threads).mapToObj(i -> new Thread(() -> {
            ready.countDown();
            try {
                go.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            results.add(service.requeue(dead.getId()));
        })).toList();
        workers.forEach(Thread::start);
        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        for (Thread t : workers) {
            t.join(5000);
        }

        long requeued = results.stream().filter(RequeueResult::requeued).count();
        long rejected = results.stream()
                .filter(r -> r.outcome() == RequeueOutcome.NOT_DEAD_LETTER).count();
        EventRecord after = store.findById(dead.getId()).orElseThrow();
        log.info("并发重投: idempotencyKey={} 生效={} 拒绝={} requeueCount={} status={}",
                after.getIdempotencyKey(), requeued, rejected,
                after.getRequeueCount(), after.getStatus());
        assertThat(results).hasSize(threads);
        assertThat(requeued).isEqualTo(1);
        assertThat(rejected).isEqualTo(threads - 1);
        assertThat(after.getRequeueCount()).isEqualTo(1);
        assertThat(after.getLastRequeuedAt()).isNotNull();
        assertThat(after.getStatus()).isEqualTo(EventStatus.PENDING);
    }

    @Test
    void requeueIsRejectedForNonDeadLetterAndUnknownEvents() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        EventService service = new EventService(store, fastProperties());

        // 仍在正常处理中（PENDING）
        service.submit("pending-key", "order-1", "p");
        EventRecord pending = store.findByIdempotencyKey("pending-key").orElseThrow();
        RequeueResult pendingResult = service.requeue(pending.getId());
        log.info("重投处理中事件: outcome={} reason={}", pendingResult.outcome(), pendingResult.reason());
        assertThat(pendingResult.outcome()).isEqualTo(RequeueOutcome.NOT_DEAD_LETTER);
        assertThat(pendingResult.reason()).contains(EventStatus.PENDING.name());

        // 已投递成功（DELIVERED）
        service.submit("done-key", "order-1", "d");
        EventRecord done = store.findByIdempotencyKey("done-key").orElseThrow();
        store.claim(done.getId(), done.getVersion());
        store.markDelivered(done.getId(), done.getVersion());
        RequeueResult doneResult = service.requeue(done.getId());
        log.info("重投已成功事件: outcome={} reason={}", doneResult.outcome(), doneResult.reason());
        assertThat(doneResult.outcome()).isEqualTo(RequeueOutcome.NOT_DEAD_LETTER);
        assertThat(doneResult.reason()).contains(EventStatus.DELIVERED.name());

        // 不存在
        RequeueResult missing = service.requeue("no-such-id");
        assertThat(missing.outcome()).isEqualTo(RequeueOutcome.NOT_FOUND);

        // 拒绝不产生任何状态变化
        assertThat(store.findById(pending.getId()).orElseThrow().getRequeueCount()).isZero();
        assertThat(store.findById(done.getId()).orElseThrow().getRequeueCount()).isZero();
    }

    @Test
    void requeueBeyondQuotaIsRejectedAndReasonRetained() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        props.setMaxRequeues(1);
        EventService service = new EventService(store, props);

        EventRecord dead = deadLetter(service, store, "quota-key", "order-q");

        // 第一次重投生效
        RequeueResult first = service.requeue(dead.getId());
        assertThat(first.outcome()).isEqualTo(RequeueOutcome.REQUEUED);
        // 再次投递失败回到死信
        EventRecord pending = store.findById(dead.getId()).orElseThrow();
        assertThat(pending.getAttempts()).isZero(); // 重投后重新计数
        store.claim(pending.getId(), pending.getVersion());
        store.markDead(pending.getId(), pending.getVersion(), pending.getMaxAttempts(),
                "still failing after requeue");

        // 超过配额：拒绝并保留原因
        RequeueResult second = service.requeue(dead.getId());
        EventRecord after = store.findById(dead.getId()).orElseThrow();
        log.info("配额超限: idempotencyKey={} outcome={} reason={} requeueCount={} lastError={}",
                after.getIdempotencyKey(), second.outcome(), second.reason(),
                after.getRequeueCount(), after.getLastError());
        assertThat(second.outcome()).isEqualTo(RequeueOutcome.QUOTA_EXCEEDED);
        assertThat(second.reason()).contains("maxRequeues=1");
        assertThat(after.getRequeueCount()).isEqualTo(1);
        assertThat(after.getRemainingRequeues()).isZero();
        assertThat(after.getStatus()).isEqualTo(EventStatus.DEAD_LETTER);
        assertThat(after.getLastError()).isEqualTo("still failing after requeue");
    }

    @Test
    void batchRequeueRespectsLimitAndSkipsQuotaExhausted() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        props.setMaxRequeues(3);
        props.setRequeueBatchMax(2);
        EventService service = new EventService(store, props);

        for (int i = 0; i < 4; i++) {
            deadLetter(service, store, "batch-key-" + i, "order-b" + i);
        }
        // 把 batch-key-0 的已用重投次数直接刷到上限，验证配额满的跳过且不占批量额度
        EventRecord exhausted = store.findByIdempotencyKey("batch-key-0").orElseThrow();
        exhausted.setRequeueCount(props.getMaxRequeues());

        // 请求 10 条，但被配置上限 requeueBatchMax=2 截断
        List<RequeueResult> results = service.requeueBatch(10);
        long requeued = results.stream().filter(RequeueResult::requeued).count();
        long quotaRejected = results.stream()
                .filter(r -> r.outcome() == RequeueOutcome.QUOTA_EXCEEDED).count();
        log.info("批量重投: 生效={} 配额拒绝={} 明细={}", requeued, quotaRejected,
                results.stream().map(r -> r.event().getIdempotencyKey() + ":" + r.outcome()).toList());
        assertThat(requeued).isEqualTo(2);
        assertThat(quotaRejected).isEqualTo(1);
        assertThat(store.countByStatus().get(EventStatus.DEAD_LETTER)).isEqualTo(2L);
        assertThat(store.countByStatus().get(EventStatus.PENDING)).isEqualTo(2L);

        // 重投统计可见（含被刷到上限的 batch-key-0 已用 3 次）
        var stats = store.requeueStats();
        log.info("重投统计: totalRequeues={} requeuedEvents={}",
                stats.totalRequeues(), stats.requeuedEvents());
        assertThat(stats.totalRequeues()).isEqualTo(5L);
        assertThat(stats.requeuedEvents()).isEqualTo(3L);
    }

    @Test
    void laterEventWaitsWhileHeadOfOrderKeyIsBackingOff() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        props.setBackoffBaseMs(300);
        props.setBackoffMaxMs(600);
        EventService service = new EventService(store, props);

        // A1 第一次消费失败进入退避，之后成功；A2 若先于 A1 成功被消费即违规
        ConcurrentLinkedQueue<String> consumed = new ConcurrentLinkedQueue<>();
        AtomicInteger a1Consumptions = new AtomicInteger();
        AtomicBoolean violation = new AtomicBoolean();
        EventConsumer consumer = event -> {
            if ("A1".equals(event.getPayload()) && a1Consumptions.incrementAndGet() == 1) {
                throw new IllegalStateException("A1 first attempt fails");
            }
            if ("A2".equals(event.getPayload())
                    && store.findByIdempotencyKey("key-a1").orElseThrow().getStatus()
                    != EventStatus.DELIVERED) {
                violation.set(true);
            }
            consumed.add(event.getPayload());
        };
        EventDispatcher dispatcher = new EventDispatcher(store, consumer, props);
        dispatcher.start();
        try {
            service.submit("key-a1", "order-hb", "A1");
            service.submit("key-a2", "order-hb", "A2");

            // 等 A1 第一次失败进入退避等待
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                EventRecord a1 = store.findByIdempotencyKey("key-a1").orElseThrow();
                return a1.getStatus() == EventStatus.PENDING && a1.getLastError() != null;
            });
            // 退避窗口内（300ms 级）A2 不得被调度
            assertThat(consumed).doesNotContain("A2");

            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store.countByStatus().get(EventStatus.DELIVERED) == 2L);
            log.info("退避等待期同键阻塞: idempotencyKey=key-a1 attempts={} consumed={} violation={}",
                    store.findByIdempotencyKey("key-a1").orElseThrow().getAttempts(),
                    consumed, violation.get());
            assertThat(violation).isFalse();
            // A1 第一次失败不计入 consumed（消费回调在记录前抛异常）
            assertThat(consumed).containsExactly("A1", "A2");
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    void requeuedEventKeepsOriginalPositionWithinOrderKey() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        PipelineProperties props = fastProperties();
        props.setMaxAttempts(2);
        // 固定 500ms 退避，给“死信解锁 -> A2 首次失败 -> 重投 A1”留出可观测窗口
        props.setBackoffBaseMs(500);
        props.setBackoffMaxMs(500);
        EventService service = new EventService(store, props);

        // A1 前两次消费失败（进死信）；A2 第一次失败（进入退避）；其余成功
        Map<String, AtomicInteger> consumptions = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<String> deliveredOrder = new ConcurrentLinkedQueue<>();
        EventConsumer consumer = event -> {
            int n = consumptions.computeIfAbsent(event.getPayload(),
                    k -> new AtomicInteger()).incrementAndGet();
            if ("A1".equals(event.getPayload()) && n <= 2) {
                throw new IllegalStateException("A1 poison #" + n);
            }
            if ("A2".equals(event.getPayload()) && n == 1) {
                throw new IllegalStateException("A2 first attempt fails");
            }
            deliveredOrder.add(event.getPayload());
        };
        EventDispatcher dispatcher = new EventDispatcher(store, consumer, props);
        dispatcher.start();
        try {
            service.submit("key-a1", "order-pos", "A1");
            service.submit("key-a2", "order-pos", "A2");

            // A1 第一次失败进入退避等待：此期间 A2 不得被消费（前一条未成功也未进死信）
            await().pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(5)).until(() -> {
                EventRecord a1 = store.findByIdempotencyKey("key-a1").orElseThrow();
                return a1.getStatus() == EventStatus.PENDING && a1.getLastError() != null;
            });
            assertThat(consumptions).doesNotContainKey("A2");

            // A1 进死信后 A2 解锁；等 A2 第一次失败进入退避等待，此时重投 A1
            await().pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(5)).until(() ->
                    store.findByIdempotencyKey("key-a1").orElseThrow().getStatus()
                            == EventStatus.DEAD_LETTER);
            await().pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(5)).until(() -> {
                EventRecord a2 = store.findByIdempotencyKey("key-a2").orElseThrow();
                return a2.getStatus() == EventStatus.PENDING && a2.getLastError() != null;
            });
            RequeueResult requeue = service.requeue(
                    store.findByIdempotencyKey("key-a1").orElseThrow().getId());
            assertThat(requeue.outcome()).isEqualTo(RequeueOutcome.REQUEUED);
            log.info("重投归位: idempotencyKey=key-a1 requeueCount={} status={}",
                    requeue.event().getRequeueCount(), requeue.event().getStatus());

            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store.countByStatus().get(EventStatus.DELIVERED) == 2L);
            EventRecord a1 = store.findByIdempotencyKey("key-a1").orElseThrow();
            log.info("重投后顺序: idempotencyKey=key-a1 requeueCount={} attempts={} deliveredOrder={}",
                    a1.getRequeueCount(), a1.getAttempts(), deliveredOrder);
            // A1 重新入队后仍然先于 A2 完成
            assertThat(deliveredOrder).containsExactly("A1", "A2");
            assertThat(a1.getRequeueCount()).isEqualTo(1);
        } finally {
            dispatcher.stop();
        }
    }

    @Test
    void requeueSurvivesRestartAndDeliveredEventsAreNotRedelivered() {
        Path file = dir.resolve("events.jsonl");
        PipelineProperties props = fastProperties();

        // 第一次运行：一条死信后重投生效（PENDING），一条已投递成功，随后“进程崩溃”
        FileEventStore store1 = new FileEventStore(file);
        EventService service1 = new EventService(store1, props);
        EventRecord dead = deadLetter(service1, store1, "key-rq", "order-rs");
        RequeueResult requeue = service1.requeue(dead.getId());
        assertThat(requeue.outcome()).isEqualTo(RequeueOutcome.REQUEUED);
        service1.submit("key-done", "order-rs2", "done");
        EventRecord done = store1.findByIdempotencyKey("key-done").orElseThrow();
        store1.claim(done.getId(), done.getVersion());
        store1.markDelivered(done.getId(), done.getVersion());
        log.info("模拟崩溃: idempotencyKey={} status={} requeueCount={}",
                dead.getIdempotencyKey(),
                store1.findById(dead.getId()).orElseThrow().getStatus(),
                store1.findById(dead.getId()).orElseThrow().getRequeueCount());

        // 重启：重放文件，重投次数与重投时间保留，未完成的重投事件继续投递
        FileEventStore store2 = new FileEventStore(file);
        EventRecord restored = store2.findById(dead.getId()).orElseThrow();
        assertThat(restored.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(restored.getRequeueCount()).isEqualTo(1);
        assertThat(restored.getLastRequeuedAt()).isNotNull();

        ConcurrentLinkedQueue<String> consumed = new ConcurrentLinkedQueue<>();
        EventDispatcher dispatcher = new EventDispatcher(store2,
                event -> consumed.add(event.getIdempotencyKey()), props);
        dispatcher.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    store2.findById(dead.getId()).orElseThrow().getStatus()
                            == EventStatus.DELIVERED);
            EventRecord finished = store2.findById(dead.getId()).orElseThrow();
            log.info("重启后重投事件完成: idempotencyKey={} attempts={} requeueCount={} consumed={}",
                    finished.getIdempotencyKey(), finished.getAttempts(),
                    finished.getRequeueCount(), consumed);
            // 重投事件被恰好投递一次，已成功的 key-done 不被重复投递
            assertThat(consumed).containsExactly("key-rq");
            assertThat(finished.getRequeueCount()).isEqualTo(1);
        } finally {
            dispatcher.stop();
        }
    }
}
