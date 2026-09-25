package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.handler.SimulatedDeliveryHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

/**
 * 集成测试基类：每个测试类/上下文使用独立临时数据目录，避免存储相互污染。
 */
public abstract class AbstractPipelineIntegrationTest {

    @TempDir
    static Path sharedTempDir;

    @DynamicPropertySource
    static void pipelineProps(DynamicPropertyRegistry registry) {
        registry.add("pipeline.data-dir", () -> sharedTempDir.toString());
    }

    @BeforeEach
    void clearSimulatedDownstream() {
        SimulatedDeliveryHandler.clearTestState();
    }
}
