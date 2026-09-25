package com.github.xingjianluo.idempotenteventpipelinejava.pipeline;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.handler.DeliveryHandler;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 异步投递 worker。
 *
 * <ul>
 *   <li>单线程调度器按固定间隔从存储领取到期事件，领取事件时即把状态落盘为 PROCESSING；</li>
 *   <li>每个顺序键绑定一个单线程执行器：相同顺序键严格 FIFO，不同顺序键并行、互不阻塞；</li>
 *   <li>存储层保证“同键有 PROCESSING 时不领取后续事件、队首退避未到期时阻塞同键后续事件”；</li>
 *   <li>启动时把租约不属于本 worker 的残留 PROCESSING（上次进程崩溃/被 kill）退回 PENDING 继续投递；</li>
 *   <li>失败按指数退避安排下一次尝试，尝试次数达到上限后进入死信并保留最后失败原因。</li>
 * </ul>
 */
@Component
public class PipelineWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(PipelineWorker.class);

    private static final long STOP_AWAIT_SECONDS = 10;

    private final EventStore store;
    private final DeliveryHandler handler;
    private final PipelineProperties properties;

    /** 本进程 worker 的唯一标识，用于租约与重启恢复判定。 */
    private final String workerId = UUID.randomUUID().toString();

    /** 顺序键 -> 串行投递执行器。 */
    private final ConcurrentHashMap<String, ExecutorService> keyExecutors = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public PipelineWorker(EventStore store, DeliveryHandler handler, PipelineProperties properties) {
        this.store = store;
        this.handler = handler;
        this.properties = properties;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        int recovered = store.recoverStaleProcessing(workerId, Instant.now());
        log.info("投递 worker 启动 workerId={} 恢复残留 PROCESSING 事件 {} 条", workerId, recovered);

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pipeline-poller");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::pollSafely,
                0L, properties.getPollIntervalMs(), TimeUnit.MILLISECONDS);
        running = true;
    }

    private void pollSafely() {
        try {
            poll();
        } catch (Throwable t) {
            // 轮询线程绝不能因为单次异常而死掉
            log.error("轮询投递发生未预期异常，下一轮继续", t);
        }
    }

    private void poll() {
        List<EventRecord> due = store.claimDue(workerId, properties.getClaimBatchSize(), Instant.now());
        for (EventRecord claimed : due) {
            ExecutorService executor = executorFor(claimed.getOrderKey());
            executor.submit(() -> deliver(claimed));
        }
    }

    private void deliver(EventRecord claimed) {
        long seq = claimed.getSeq();
        String key = claimed.getIdempotencyKey();
        int attempt = claimed.getAttempt();
        try {
            handler.handle(claimed);
        } catch (Throwable t) {
            String reason = t.getClass().getSimpleName() + ": " + t.getMessage();
            long backoffMs = backoffMsForAttempt(attempt);
            EventStore.FailureMark mark = store.markFailed(
                    seq, workerId, reason, properties.getMaxAttempts(), backoffMs, Instant.now());
            if (mark == null) {
                log.warn("失败登记未生效（租约已失效） idempotencyKey={} seq={}", key, seq);
            }
            return;
        }
        boolean confirmed = store.markDelivered(seq, workerId, Instant.now());
        if (!confirmed) {
            log.warn("成功确认未生效（租约已失效） idempotencyKey={} seq={}", key, seq);
        }
    }

    /**
     * 第 {@code failedAttempt} 次尝试失败后的退避时长：
     * initial * multiplier^(failedAttempt-1)，并以 backoffMaxMs 封顶。
     */
    long backoffMsForAttempt(int failedAttempt) {
        double value = properties.getBackoffInitialMs()
                * Math.pow(properties.getBackoffMultiplier(), Math.max(0, failedAttempt - 1));
        return Math.min(properties.getBackoffMaxMs(), Math.round(value));
    }

    private ExecutorService executorFor(String orderKey) {
        return keyExecutors.computeIfAbsent(orderKey, k ->
                Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "pipeline-delivery-" + k);
                    t.setDaemon(true);
                    return t;
                }));
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        scheduler.shutdownNow();
        for (ExecutorService executor : keyExecutors.values()) {
            executor.shutdownNow();
        }
        try {
            scheduler.awaitTermination(STOP_AWAIT_SECONDS, TimeUnit.SECONDS);
            for (ExecutorService executor : keyExecutors.values()) {
                executor.awaitTermination(STOP_AWAIT_SECONDS, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        keyExecutors.clear();
        log.info("投递 worker 已停止 workerId={}", workerId);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public String getWorkerId() {
        return workerId;
    }
}
