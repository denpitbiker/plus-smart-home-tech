package ru.yandex.practicum.collector;

import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "grpc.server.port=0")
@Import(GrpcTestConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class UnavailableKafkaIT {
    @Autowired AtomicInteger grpcPort;
    private static ServerSocket offlineListener;

    @DynamicPropertySource
    static void offline(DynamicPropertyRegistry registry) throws IOException {
        // Reserve a non-Kafka listener so no broker can bind to the test address.
        offlineListener = new ServerSocket(0);
        registry.add("collector.kafka.bootstrap-servers", () -> "localhost:" + offlineListener.getLocalPort());
    }

    @AfterAll
    static void closeListener() throws IOException {
        if (offlineListener != null) {
            offlineListener.close();
        }
    }

    @Test
    void returnsUnavailableInsteadOfSuccess() throws InterruptedException {
        var channel = ManagedChannelBuilder.forAddress("localhost", grpcPort.get()).usePlaintext().build();
        try {
            var stub = CollectorControllerGrpc.newBlockingStub(channel).withDeadlineAfter(40, TimeUnit.SECONDS);
            var event = (SensorEventProto) GrpcEventFixtures.events().get(3).request();
            assertThatThrownBy(() -> stub.collectSensorEvent(event))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            error -> assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
        } finally {
            channel.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
