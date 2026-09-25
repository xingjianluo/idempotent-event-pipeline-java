package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.SubmitOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.store.FileEventStore;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 存储层单测：幂等提交、并发区分、FIFO 领取、CAS 状态迁移与重启恢复。
 */
class FileEventStoreTest {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FileEventStoreTest.class);

    @TempDir
    Path tempDir;

    private FileEventStore store;

    @BeforeEach
    void setUp() {
        PipelineProperties props = new PipelineProperties();
        props.setDataDir(tempDir.toString());
        store = new FileEventStore(props);
        store.initialize();
    }

    @AfterEach
    void tearDown() {
        log.info("存储层用例结束，快照文件仍在 {}", tempDir);
    }

    @Test
    void concurrentSameKey_onlyOneAccepted_otherRejectedAsConcurrent() throws Exception {
        String key = "idem-concurrent-1";
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        List<SubmitOutcome> outcomes = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    EventStore.SubmitResult r = store.submit(key, "order-A", "p", Instant.now());
                    outcomes.add(r.getOutcome());
                    switch (r.getOutcome()) {
                        case ACCEPTED -> accepted.incrementAndGet();
                        case REJECTED_CONCURRENT -> rejected.incrementAndGet();
                        default -> other.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        log.info("并发重复提交 idempotencyKey={} ACCEPTED={} REJECTED_CONCURRENT={} 其它={} 明细={}",
                key, accepted.get(), rejected.get(), other.get(), outcomes);

        assertEquals(1, accepted.get(), "同一幂等键只能有一个请求生效");
        assertEquals(threads - 1, rejected.get(), "其余并发请求必须得到可区分的并发拒绝");
        assertEquals(0, other.get());
        assertEquals(1, store.findAll().size(), "存储中只允许有一条事件");
    }

    @Test
    void sequentialDuplicateAfterInflight_isDuplicateInflight_afterDelivered_isReused() {
        String key = "idem-seq-1";
        EventStore.SubmitResult first = store.submit(key, "o", "p", Instant.now());
        assertEquals(SubmitOutcome.ACCEPTED, first.getOutcome());
        log.info("顺序重复 idempotencyKey={} 首次提交 seq={}", key, first.getEvent().getSeq());

        EventStore.SubmitResult dup = store.submit(key, "o", "p", Instant.now());
        log.info("顺序重复 idempotencyKey={} 在途时重复结论={}", key, dup.getOutcome());
        assertEquals(SubmitOutcome.DUPLICATE_INFLIGHT, dup.getOutcome(),
                "临界区结束后的重复提交应判定为在途重复，而非并发拒绝");

        List<EventRecord> claimed = store.claimDue("w1", 10, Instant.now());
        assertEquals(1, claimed.size());
        assertTrue(store.markDelivered(claimed.get(0).getSeq(), "w1", Instant.now()));

        EventStore.SubmitResult reuse = store.submit(key, "o", "p", Instant.now());
        log.info("顺序重复 idempotencyKey={} 完成后重复结论={}", key, reuse.getOutcome());
        assertEquals(SubmitOutcome.REUSED_DELIVERED, reuse.getOutcome());
        assertEquals(1, store.findAll().size());
    }

    @Test
    void claim_fifoPerOrderKey_headBackoffBlocksKey_otherKeysNotBlocked() {
        // 同一顺序键 K 两条事件 + 另一顺序键 J 一条
        store.submit("a1", "K", "p", Instant.now());
        store.submit("a2", "K", "p", Instant.now().plusMillis(1));
        store.submit("b1", "J", "p", Instant.now());

        // 第一次领取：K 键队首 a1 与 J 键 b1 可领取（a2 因同键已有 PROCESSING 被跳过）
        List<EventRecord> first = store.claimDue("w", 10, Instant.now());
        assertEquals(2, first.size());
        EventRecord a1 = first.stream().filter(e -> e.getIdempotencyKey().equals("a1")).findFirst().orElseThrow();
        EventRecord b1 = first.stream().filter(e -> e.getIdempotencyKey().equals("b1")).findFirst().orElseThrow();

        // a1 失败并安排较长退避；b1 成功
        EventStore.FailureMark mark = store.markFailed(a1.getSeq(), "w", "transient-boom", 3, 60_000, Instant.now());
        assertEquals(EventStatus.PENDING, mark.getNewStatus());
        assertTrue(store.markDelivered(b1.getSeq(), "w", Instant.now()));

        // 立即再领取：K 键被队首 a1 的退避阻塞，a2 也不能越过队首；J 键已空 -> 无事件可领
        List<EventRecord> blocked = store.claimDue("w", 10, Instant.now());
        log.info("队首退避期间领取到的键={}（期望为空）",
                blocked.stream().map(EventRecord::getIdempotencyKey).toList());
        assertTrue(blocked.isEmpty(), "同键队首退避未到期时，后续事件不得越过它");

        // 退避到期：只能先领取队首 a1，仍不能是 a2
        List<EventRecord> retry = store.claimDue("w", 10, Instant.now().plusMillis(60_001));
        assertEquals(1, retry.size());
        assertEquals("a1", retry.get(0).getIdempotencyKey(), "退避到期后仍需按 FIFO 先投递队首");
        assertTrue(store.markDelivered(retry.get(0).getSeq(), "w", Instant.now()));

        // a1 完成后 a2 才允许投递
        List<EventRecord> tail = store.claimDue("w", 10, Instant.now());
        assertEquals(1, tail.size());
        assertEquals("a2", tail.get(0).getIdempotencyKey());
    }

    @Test
    void markFailed_movesToDeadLetterAfterMaxAttempts_andKeepsReason() {
        store.submit("d1", "o", "p", Instant.now());
        int maxAttempts = 3;
        String owner = "w1";
        String lastReason = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            List<EventRecord> claimed = store.claimDue(owner, 10,
                    Instant.now().plusMillis(100_000L * attempt));
            assertEquals(1, claimed.size());
            assertEquals(attempt, claimed.get(0).getAttempt());
            lastReason = "ERR-" + attempt;
            EventStore.FailureMark mark = store.markFailed(claimed.get(0).getSeq(), owner, lastReason,
                    maxAttempts, 50, Instant.now());
            assertNotNull(mark);
            log.info("idempotencyKey=d1 attempt={} 状态迁移=>{}", attempt, mark.getNewStatus());
            if (attempt < maxAttempts) {
                assertEquals(EventStatus.PENDING, mark.getNewStatus());
            } else {
                assertEquals(EventStatus.DEAD_LETTER, mark.getNewStatus());
            }
        }
        EventRecord dead = store.findByKey("d1");
        assertEquals(EventStatus.DEAD_LETTER, dead.getStatus());
        assertEquals("ERR-3", dead.getLastFailureReason(), "死信必须保留最后一次失败原因");
        assertNull(dead.getLeaseOwner());
        log.info("idempotencyKey=d1 终态={} attempt={} 最后原因={}",
                dead.getStatus(), dead.getAttempt(), dead.getLastFailureReason());
    }

    @Test
    void cas_ownerMismatchRejectsCompletion() {
        store.submit("c1", "o", "p", Instant.now());
        store.claimDue("owner-A", 10, Instant.now());
        assertFalseDeliver("owner-B");
        assertTrue(store.markDelivered(store.findByKey("c1").getSeq(), "owner-A", Instant.now()),
                "只有租约持有者才能确认成功");
    }

    private void assertFalseDeliver(String wrongOwner) {
        boolean ok = store.markDelivered(store.findByKey("c1").getSeq(), wrongOwner, Instant.now());
        org.junit.jupiter.api.Assertions.assertFalse(ok, "错误的租约持有者不得完成事件");
    }

    @Test
    void staleProcessing_recoveredOnRestartByNewOwner() {
        store.submit("r1", "o", "p", Instant.now());
        store.claimDue("old-owner", 10, Instant.now());
        log.info("idempotencyKey=r1 旧进程领取后崩溃，状态={} lease=old-owner", EventStatus.PROCESSING);

        // 模拟新进程启动恢复：构造一个指向同一目录的新存储实例
        PipelineProperties props = new PipelineProperties();
        props.setDataDir(tempDir.toString());
        FileEventStore restarted = new FileEventStore(props);
        restarted.initialize();
        int recovered = restarted.recoverStaleProcessing("new-owner", Instant.now());
        assertEquals(1, recovered);
        EventRecord r1 = restarted.findByKey("r1");
        assertEquals(EventStatus.PENDING, r1.getStatus());
        assertNull(r1.getLeaseOwner());
        log.info("idempotencyKey=r1 重启恢复后状态={} attempt={}（不会重复完成，也不会被丢弃）",
                r1.getStatus(), r1.getAttempt());

        List<EventRecord> claimed = restarted.claimDue("new-owner", 10, Instant.now());
        assertEquals(1, claimed.size());
        assertEquals(2, claimed.get(0).getAttempt(), "恢复后再次投递时尝试次数继续累加");
    }

    @Test
    void pendingEventWaitingBackoff_becomesImmediatelyDueOnRestart() {
        store.submit("p1", "o", "p", Instant.now());
        // 第一次投递失败并安排 60 秒后退避
        List<EventRecord> claimed = store.claimDue("old-owner", 10, Instant.now());
        assertEquals(1, claimed.size());
        store.markFailed(claimed.get(0).getSeq(), "old-owner", "boom", 3, 60_000, Instant.now());
        assertTrue(store.claimDue("old-owner", 10, Instant.now()).isEmpty(),
                "退避未到期前不可领取");

        // 新进程启动恢复：退避应被重置，立即可领取
        PipelineProperties props = new PipelineProperties();
        props.setDataDir(tempDir.toString());
        FileEventStore restarted = new FileEventStore(props);
        restarted.initialize();
        Instant now = Instant.now();
        int recovered = restarted.recoverStaleProcessing("new-owner", now);
        assertEquals(1, recovered);
        List<EventRecord> due = restarted.claimDue("new-owner", 10, now);
        assertEquals(1, due.size());
        assertEquals("p1", due.get(0).getIdempotencyKey());
        log.info("idempotencyKey=p1 重启后退避被重置，立即重新投递 attempt={}", due.get(0).getAttempt());
    }
}
