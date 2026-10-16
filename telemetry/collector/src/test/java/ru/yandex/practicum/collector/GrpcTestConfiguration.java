package ru.yandex.practicum.collector;

import java.util.concurrent.atomic.AtomicInteger;
import net.devh.boot.grpc.server.event.GrpcServerStartedEvent;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;

@TestConfiguration(proxyBeanMethods = false)
public class GrpcTestConfiguration {
    @Bean
    AtomicInteger grpcPort() {
        return new AtomicInteger();
    }

    @Bean
    ApplicationListener<GrpcServerStartedEvent> portListener(AtomicInteger grpcPort) {
        return event -> grpcPort.set(event.getServer().getPort());
    }
}
