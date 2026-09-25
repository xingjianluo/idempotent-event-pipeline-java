package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.model.SubmitResult;
import com.github.xingjianluo.idempotenteventpipelinejava.service.EventService;
import com.github.xingjianluo.idempotenteventpipelinejava.store.FileEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 并发重复提交：同一幂等键只能有一个请求生效，其余得到可区分的 DUPLICATE 结果。
 */
class ConcurrentSubmitTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmitTest.class);

    @TempDir
    Path dir;

    @Test
    void concurrentSubmitsWithSameKeyProduceExactlyOneAccepted() throws Exception {
        FileEventStore store = new FileEventStore(dir.resolve("events.jsonl"));
        EventService service = new EventService(store, new PipelineProperties());

        int threads = 32;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Callable<SubmitResult>> tasks = IntStream.range(0, threads)
                .mapToObj(i -> (Callable<SubmitResult>) () -> {
                    ready.countDown();
                    go.await();
                    return service.submit("shared-key", "order-1", "payload-" + i);
                })
                .toList();

        List<Future<SubmitResult>> futures = tasks.stream().map(pool::submit).toList();
        ready.await();
        go.countDown();

        List<SubmitResult> results = futures.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).toList();
        pool.shutdown();

        long accepted = results.stream().filter(SubmitResult::accepted).count();
        long duplicates = results.stream()
                .filter(r -> r.outcome() == SubmitOutcome.DUPLICATE).count();
        String acceptedId = results.stream().filter(SubmitResult::accepted)
                .findFirst().orElseThrow().event().getId();
        boolean allReuseSameEvent = results.stream()
                .allMatch(r -> r.event().getId().equals(acceptedId));

        log.info("并发提交结果: idempotencyKey=shared-key accepted={} duplicates={} 复用同一事件={} eventId={}",
                accepted, duplicates, allReuseSameEvent, acceptedId);

        assertThat(accepted).isEqualTo(1);
        assertThat(duplicates).isEqualTo(threads - 1);
        assertThat(allReuseSameEvent).isTrue();
        assertThat(store.countByStatus().get(EventStatus.PENDING)).isEqualTo(1L);
    }
}
