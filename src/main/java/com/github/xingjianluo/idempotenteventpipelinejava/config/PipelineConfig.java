package com.github.xingjianluo.idempotenteventpipelinejava.config;

import com.github.xingjianluo.idempotenteventpipelinejava.pipeline.EventConsumer;
import com.github.xingjianluo.idempotenteventpipelinejava.pipeline.EventDispatcher;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import com.github.xingjianluo.idempotenteventpipelinejava.store.FileEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 投递链路装配：本地文件存储 + 默认消费者 + 调度器生命周期。
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    private static final Logger log = LoggerFactory.getLogger(PipelineConfig.class);

    @Bean
    public EventStore eventStore(PipelineProperties properties) {
        return new FileEventStore(Path.of(properties.getStorePath()));
    }

    /**
     * 默认消费者：仅记录日志。
     * 负载中包含 "FAIL" 时抛异常，用于本地演示重试与死信。
     */
    @Bean
    public EventConsumer eventConsumer() {
        return event -> {
            if (event.getPayload() != null && event.getPayload().contains("FAIL")) {
                throw new IllegalStateException("模拟消费失败: payload=" + event.getPayload());
            }
            log.info("消费事件: id={} idempotencyKey={} payload={}",
                    event.getId(), event.getIdempotencyKey(), event.getPayload());
        };
    }

    @Bean(destroyMethod = "stop")
    public EventDispatcher eventDispatcher(EventStore store, EventConsumer consumer,
                                           PipelineProperties properties) {
        EventDispatcher dispatcher = new EventDispatcher(store, consumer, properties);
        dispatcher.start();
        return dispatcher;
    }
}
