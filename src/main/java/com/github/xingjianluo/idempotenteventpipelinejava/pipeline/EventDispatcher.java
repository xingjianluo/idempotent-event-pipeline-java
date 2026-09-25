package com.github.xingjianluo.idempotenteventpipelinejava.pipeline;

import com.github.xingjianluo.idempotenteventpipelinejava.config.PipelineProperties;
import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 投递调度器。
 *
 * <p>语义：</p>
 * <ul>
 *   <li>启动时先把残留的 IN_FLIGHT 事件恢复为 PENDING，保证重启后未完成事件继续投递；</li>
 *   <li>相同顺序键的事件同一时刻最多投递一条，严格按提交先后保持相对顺序；</li>
 *   <li>不同顺序键互不阻塞，并行投递；</li>
 *   <li>消费失败按指数退避重试，超过最大尝试次数进入死信并保留最后一次失败原因。</li>
 * </ul>
 */
public class EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

    private static final int BATCH_SIZE = 100;

    private final EventStore store;
    private final EventConsumer consumer;
    private final PipelineProperties properties;

    /** 正在投递中的顺序键，保证同键串行。 */
    private final Set<String> inFlightOrderKeys = ConcurrentHashMap.newKeySet();

    private ScheduledExecutorService scheduler;
    private ExecutorService workers;
    private volatile boolean running;

    public EventDispatcher(EventStore store, EventConsumer consumer, PipelineProperties properties) {
        this.store = store;
        this.consumer = consumer;
        this.properties = properties;
    }

    public void start() {
        store.recoverInFlight();
        workers = Executors.newVirtualThreadPerTaskExecutor();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "event-dispatcher");
            t.setDaemon(true);
            return t;
        });
        running = true;
        scheduler.scheduleWithFixedDelay(this::pollSafely,
                0, properties.getPollIntervalMs(), TimeUnit.MILLISECONDS);
        log.info("调度器已启动: pollIntervalMs={} maxAttempts={} backoffBaseMs={} backoffMaxMs={}",
                properties.getPollIntervalMs(), properties.getMaxAttempts(),
                properties.getBackoffBaseMs(), properties.getBackoffMaxMs());
    }

    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (workers != null) {
            workers.shutdown();
            try {
                workers.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("调度器已停止");
    }

    private void pollSafely() {
        try {
            poll();
        } catch (Throwable t) {
            log.error("调度轮询异常", t);
        }
    }

    private void poll() {
        List<EventRecord> due = store.findDue(Instant.now(), BATCH_SIZE);
        for (EventRecord event : due) {
            if (!running) {
                return;
            }
            // 同一顺序键已有事件在投递：跳过，等下一轮，保证同键串行。
            if (!inFlightOrderKeys.add(event.getOrderKey())) {
                continue;
            }
            workers.submit(() -> {
                try {
                    deliver(event);
                } finally {
                    inFlightOrderKeys.remove(event.getOrderKey());
                }
            });
        }
    }

    private void deliver(EventRecord event) {
        long version = event.getVersion();
        if (!store.claim(event.getId(), version)) {
            return; // 已被其他流程认领或状态变化，放弃本次投递
        }
        int attempt = event.getAttempts() + 1;
        event.setAttempts(attempt);
        log.info("开始投递: id={} idempotencyKey={} orderKey={} attempt={}/{}",
                event.getId(), event.getIdempotencyKey(), event.getOrderKey(),
                attempt, event.getMaxAttempts());
        try {
            consumer.consume(event);
            store.markDelivered(event.getId(), event.getVersion());
            log.info("投递成功: id={} idempotencyKey={} attempt={}",
                    event.getId(), event.getIdempotencyKey(), attempt);
        } catch (Throwable t) {
            onFailure(event, attempt, t);
        }
    }

    private void onFailure(EventRecord event, int attempt, Throwable t) {
        String reason = t.getClass().getSimpleName() + ": " + t.getMessage();
        if (attempt >= event.getMaxAttempts()) {
            store.markDead(event.getId(), event.getVersion(), attempt, reason);
            log.warn("投递失败，进入死信: id={} idempotencyKey={} attempt={} reason={}",
                    event.getId(), event.getIdempotencyKey(), attempt, reason);
            return;
        }
        long delayMs = backoffDelayMs(attempt);
        Instant nextAttemptAt = Instant.now().plusMillis(delayMs);
        store.reschedule(event.getId(), event.getVersion(), attempt, reason, nextAttemptAt);
        log.warn("投递失败，安排重试: id={} idempotencyKey={} attempt={} nextAttemptAt={} reason={}",
                event.getId(), event.getIdempotencyKey(), attempt, nextAttemptAt, reason);
    }

    /** 指数退避：base * 2^(attempt-1)，封顶 backoffMaxMs。 */
    long backoffDelayMs(int attempt) {
        long delay = properties.getBackoffBaseMs() << Math.min(attempt - 1, 20);
        return Math.min(delay, properties.getBackoffMaxMs());
    }
}
