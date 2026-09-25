package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore.InsertOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.store.FileEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 本地存储语义：幂等键唯一约束与重启恢复。
 */
class FileEventStoreTest {

    private static final Logger log = LoggerFactory.getLogger(FileEventStoreTest.class);

    @TempDir
    Path dir;

    private EventRecord newRecord(String idempotencyKey, String orderKey) {
        EventRecord record = new EventRecord();
        record.setId(UUID.randomUUID().toString());
        record.setIdempotencyKey(idempotencyKey);
        record.setOrderKey(orderKey);
        record.setPayload("payload-of-" + idempotencyKey);
        record.setStatus(EventStatus.PENDING);
        record.setAttempts(0);
        record.setMaxAttempts(3);
        record.setNextAttemptAt(Instant.now());
        return record;
    }

    @Test
    void duplicateIdempotencyKeyIsRejectedAndReusesExistingRecord() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));

        InsertOutcome first = store.insertIfAbsent(newRecord("key-1", "order-1"));
        InsertOutcome second = store.insertIfAbsent(newRecord("key-1", "order-1"));

        log.info("幂等键=key-1 首次 inserted={} 重复 inserted={} 复用事件 id={}",
                first.inserted(), second.inserted(), second.record().getId());
        assertThat(first.inserted()).isTrue();
        assertThat(second.inserted()).isFalse();
        assertThat(second.record().getId()).isEqualTo(first.record().getId());
        assertThat(store.countByStatus().get(EventStatus.PENDING)).isEqualTo(1L);
    }

    @Test
    void restartRecoversInFlightEventsBackToPending() {
        Path file = dir.resolve("events.jsonl");
        FileEventStore store = new FileEventStore(file);

        EventRecord delivered = newRecord("key-done", "order-1");
        store.insertIfAbsent(delivered);
        store.claim(delivered.getId(), delivered.getVersion());
        store.markDelivered(delivered.getId(), delivered.getVersion());

        EventRecord stuck = newRecord("key-stuck", "order-2");
        store.insertIfAbsent(stuck);
        store.claim(stuck.getId(), stuck.getVersion());
        log.info("模拟崩溃前: idempotencyKey={} status={} attempts={}",
                stuck.getIdempotencyKey(), stuck.getStatus(), stuck.getAttempts());

        // 模拟进程重启：同一文件重新加载
        FileEventStore reopened = new FileEventStore(file);
        List<EventRecord> recovered = reopened.recoverInFlight();

        assertThat(recovered).hasSize(1);
        EventRecord after = reopened.findByIdempotencyKey("key-stuck").orElseThrow();
        log.info("重启恢复后: idempotencyKey={} status={} attempts={}",
                after.getIdempotencyKey(), after.getStatus(), after.getAttempts());
        assertThat(after.getStatus()).isEqualTo(EventStatus.PENDING);
        // 已完成事件不得被重复完成或重置
        assertThat(reopened.findByIdempotencyKey("key-done").orElseThrow().getStatus())
                .isEqualTo(EventStatus.DELIVERED);
    }

    @Test
    void failureReasonIsPreservedAcrossRescheduleAndDeadLetter() {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        EventRecord record = newRecord("key-flaky", "order-1");
        store.insertIfAbsent(record);

        store.claim(record.getId(), record.getVersion());
        store.reschedule(record.getId(), record.getVersion(), 1, "boom-1",
                Instant.now().minusMillis(1));
        EventRecord retrying = store.findById(record.getId()).orElseThrow();
        log.info("重试中: idempotencyKey={} attempts={} status={} lastError={}",
                retrying.getIdempotencyKey(), retrying.getAttempts(),
                retrying.getStatus(), retrying.getLastError());
        assertThat(retrying.getStatus()).isEqualTo(EventStatus.PENDING);
        assertThat(retrying.getLastError()).isEqualTo("boom-1");

        store.claim(record.getId(), retrying.getVersion());
        store.markDead(record.getId(), record.getVersion(), 3, "boom-final");
        EventRecord dead = store.findById(record.getId()).orElseThrow();
        log.info("死信: idempotencyKey={} attempts={} status={} lastError={}",
                dead.getIdempotencyKey(), dead.getAttempts(), dead.getStatus(), dead.getLastError());
        assertThat(dead.getStatus()).isEqualTo(EventStatus.DEAD_LETTER);
        assertThat(dead.getAttempts()).isEqualTo(3);
        assertThat(dead.getLastError()).isEqualTo("boom-final");
        assertThat(store.findDeadLetters()).hasSize(1);
    }
}
