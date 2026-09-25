package com.github.xingjianluo.idempotenteventpipelinejava.handler;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * 本地可运行的模拟下游消费者。
 *
 * <p>默认所有事件直接成功；可通过管理接口（或测试代码）为指定幂等键配置：</p>
 * <ul>
 *   <li>前 N 次以临时性 / 永久性错误失败，用于验证退避重试与死信；</li>
 *   <li>在第 K 次尝试时阻塞，直到被放行，用于验证同键 FIFO 与不同键互不阻塞。</li>
 * </ul>
 *
 * <p>规则仅保存在进程内存中（模拟下游自身状态），进程重启后清空；
 * 事件本身的投递状态由持久化存储恢复。</p>
 */
@Component
public class SimulatedDeliveryHandler implements DeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDeliveryHandler.class);

    private static final long BLOCK_WAIT_SECONDS = 30;

    /** 按幂等键配置的失败规则（进程内）。 */
    private static final ConcurrentHashMap<String, FailRule> RULES = new ConcurrentHashMap<>();

    /** 按幂等键配置的阻塞点：到达指定尝试次数后，先发出“已进入”信号，再等待外部放行。 */
    private static final ConcurrentHashMap<String, BlockPoint> BLOCKS = new ConcurrentHashMap<>();

    /** 按幂等键记录实际调用次数，供测试与排查断言。 */
    private static final ConcurrentHashMap<String, Integer> INVOKED = new ConcurrentHashMap<>();

    @Override
    public void handle(EventRecord event) {
        String key = event.getIdempotencyKey();
        int attempt = event.getAttempt();
        int times = INVOKED.merge(key, 1, Integer::sum);
        log.info("下游消费开始 idempotencyKey={} orderKey={} attempt={} 本键第{}次调用",
                key, event.getOrderKey(), attempt, times);

        BlockPoint block = BLOCKS.get(key);
        if (block != null && block.atAttempt() == attempt) {
            log.warn("下游消费阻塞 idempotencyKey={} attempt={} 已进入，等待放行", key, attempt);
            block.entered().countDown();
            try {
                boolean released = block.release().await(BLOCK_WAIT_SECONDS, java.util.concurrent.TimeUnit.SECONDS);
                if (!released) {
                    throw new TransientDeliveryException(
                            "模拟下游阻塞超时 idempotencyKey=" + key + " attempt=" + attempt);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransientDeliveryException("模拟下游等待被中断 idempotencyKey=" + key, e);
            }
            log.info("下游消费放行 idempotencyKey={} attempt={}", key, attempt);
        }

        FailRule rule = RULES.get(key);
        if (rule != null && times <= rule.failTimes()) {
            String msg = "模拟下游失败 idempotencyKey=" + key + " 第" + times + "次调用/attempt=" + attempt;
            if (rule.permanent()) {
                throw new PermanentDeliveryException(msg);
            }
            throw new TransientDeliveryException(msg);
        }

        log.info("下游消费成功 idempotencyKey={} attempt={}", key, attempt);
    }

    /** 配置某幂等键前 failTimes 次投递以指定原因失败。 */
    public static void putFailRule(String key, int failTimes, boolean permanent) {
        RULES.put(key, new FailRule(failTimes, permanent));
        log.info("配置失败规则 idempotencyKey={} 前{}次失败 permanent={}", key, failTimes, permanent);
    }

    /**
     * 配置某幂等键在到达指定尝试次数时阻塞。
     * 返回的句柄中：{@code entered} 在下游刚进入处理时发出信号；{@code release} 用于外部放行。
     */
    public static BlockHandle armBlock(String key, int atAttempt) {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BLOCKS.put(key, new BlockPoint(atAttempt, entered, release));
        log.info("配置阻塞点 idempotencyKey={} attempt={}", key, atAttempt);
        return new BlockHandle(entered, release);
    }

    /** 放行指定幂等键的阻塞点。 */
    public static void releaseBlock(String key) {
        BlockPoint block = BLOCKS.get(key);
        if (block != null) {
            block.release().countDown();
            log.info("放行阻塞 idempotencyKey={}", key);
        }
    }

    public static int invokedTimes(String key) {
        return INVOKED.getOrDefault(key, 0);
    }

    public static void clearTestState() {
        RULES.clear();
        BLOCKS.clear();
        INVOKED.clear();
        log.info("已清空模拟下游的测试规则与调用计数");
    }

    record FailRule(int failTimes, boolean permanent) {
    }

    record BlockPoint(int atAttempt, CountDownLatch entered, CountDownLatch release) {
    }

    /** 阻塞点句柄：等待进入 + 放行。 */
    public record BlockHandle(CountDownLatch entered, CountDownLatch release) {
    }
}
