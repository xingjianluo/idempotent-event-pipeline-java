package com.github.xingjianluo.idempotenteventpipelinejava.pipeline;

import com.github.xingjianluo.idempotenteventpipelinejava.model.EventRecord;

/**
 * 业务消费回调：抛出异常即视为投递失败，将进入重试/死信流程。
 */
@FunctionalInterface
public interface EventConsumer {

    void consume(EventRecord event) throws Exception;
}
