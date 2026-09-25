package com.github.xingjianluo.idempotenteventpipelinejava.handler;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;

/**
 * 下游“消费者”抽象。实现类代表真正的业务投递目标。
 */
@FunctionalInterface
public interface DeliveryHandler {

    /**
     * 处理一条事件。正常返回视为投递成功；抛出
     * {@link TransientDeliveryException} 触发重试；抛出其它异常也视为失败并进入重试/死信判定。
     */
    void handle(EventRecord event) throws Exception;
}
