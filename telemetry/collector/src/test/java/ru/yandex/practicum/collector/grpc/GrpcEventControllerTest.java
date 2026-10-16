package ru.yandex.practicum.collector.grpc;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import net.devh.boot.grpc.server.event.GrpcServerStartedEvent;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import ru.yandex.practicum.collector.handler.EventDispatcher;
import ru.yandex.practicum.collector.kafka.PublicationException;
import ru.yandex.practicum.collector.service.EventCollector;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"grpc.server.port=0", "grpc.server.shutdown-grace-period=1s"})
@Import(GrpcEventControllerTest.PortConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@org.junit.jupiter.api.extension.ExtendWith(OutputCaptureExtension.class)
class GrpcEventControllerTest {
    @MockBean EventCollector collector;
    @MockBean Producer<String, SpecificRecord> producer;
    @Autowired EventDispatcher dispatcher;
    @Autowired AtomicInteger grpcPort;
    private ManagedChannel channel;
    private CollectorControllerGrpc.CollectorControllerFutureStub stub;

    @TestConfiguration(proxyBeanMethods = false)
    static class PortConfiguration {
        @Bean
        AtomicInteger grpcPort() {
            return new AtomicInteger();
        }

        @Bean
        ApplicationListener<GrpcServerStartedEvent> portListener(AtomicInteger grpcPort) {
            return event -> grpcPort.set(event.getServer().getPort());
        }
    }

    @BeforeEach
    void connect() {
        assertThat(grpcPort.get()).isPositive();
        channel = ManagedChannelBuilder.forAddress("localhost", grpcPort.get()).usePlaintext().build();
        stub = CollectorControllerGrpc.newFutureStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void closeChannel() throws InterruptedException {
        channel.shutdownNow();
        assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    private SensorEventProto sensor() {
        return SensorEventProto.newBuilder().setId("switch").setHubId("hub")
                .setTimestamp(Timestamp.newBuilder().setSeconds(100).setNanos(123456789))
                .setSwitchSensor(SwitchSensorProto.getDefaultInstance()).build();
    }

    private HubEventProto hub() {
        return HubEventProto.newBuilder().setHubId("hub")
                .setTimestamp(Timestamp.newBuilder().setSeconds(100))
                .setDeviceRemoved(DeviceRemovedEventProto.newBuilder().setId("switch")).build();
    }

    private com.google.common.util.concurrent.ListenableFuture<Empty> call(boolean sensor) {
        return sensor ? stub.collectSensorEvent(sensor()) : stub.collectHubEvent(hub());
    }

    private void returns(boolean sensor, CompletableFuture<Void> pending) {
        if (sensor) {
            when(collector.collect(any(SensorEventAvro.class))).thenReturn(pending);
        } else {
            when(collector.collect(any(HubEventAvro.class))).thenReturn(pending);
        }
    }

    private void awaitPublication(boolean sensor) {
        if (sensor) {
            verify(collector, timeout(3000)).collect(any(SensorEventAvro.class));
        } else {
            verify(collector, timeout(3000)).collect(any(HubEventAvro.class));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void respondsOnlyAfterPublicationAcknowledgement(boolean sensor) throws Exception {
        var pending = new CompletableFuture<Void>();
        returns(sensor, pending);
        var response = call(sensor);
        awaitPublication(sensor);
        assertThat(response.isDone()).isFalse();
        pending.complete(null);
        assertThat(response.get(3, TimeUnit.SECONDS)).isEqualTo(Empty.getDefaultInstance());
        if (sensor) {
            verify(collector).collect(dispatcher.map(sensor()));
        } else {
            verify(collector).collect(dispatcher.map(hub()));
        }
        verifyNoMoreInteractions(collector);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rejectsInvalidDataWithoutPublishing(boolean sensor) {
        var response = sensor ? stub.collectSensorEvent(SensorEventProto.getDefaultInstance())
                : stub.collectHubEvent(HubEventProto.getDefaultInstance());
        var error = assertThrows(ExecutionException.class, () -> response.get(3, TimeUnit.SECONDS));
        assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        verifyNoInteractions(collector);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void kafkaFailureIsUnavailableAndDoesNotLeakDetails(boolean sensor, CapturedOutput output) {
        returns(sensor, CompletableFuture.failedFuture(new CompletionException(
                new PublicationException(new IllegalStateException("private-broker-details")))));
        var error = assertThrows(ExecutionException.class, () -> call(sensor).get(3, TimeUnit.SECONDS));
        assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(Status.fromThrowable(error).getDescription()).isEqualTo("Unable to publish event");
        assertThat(output.getAll()).contains("hub=hub", "status=UNAVAILABLE", "private-broker-details");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void serializationFailureIsInternal(boolean sensor) {
        returns(sensor, CompletableFuture.failedFuture(new SerializationException("private-encoding-details")));
        var error = assertThrows(ExecutionException.class, () -> call(sensor).get(3, TimeUnit.SECONDS));
        assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(Status.fromThrowable(error).getDescription()).isEqualTo("Unable to process event");
    }

    @Test
    void synchronousPublicationFailureIsUnavailable() {
        when(collector.collect(any(SensorEventAvro.class))).thenThrow(new PublicationException(new RuntimeException("offline")));
        var error = assertThrows(ExecutionException.class, () -> call(true).get(3, TimeUnit.SECONDS));
        assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    void synchronousInternalFailureIsNotAValidationError() {
        when(collector.collect(any(SensorEventAvro.class))).thenThrow(new IllegalArgumentException("internal failure"));
        var error = assertThrows(ExecutionException.class, () -> call(true).get(3, TimeUnit.SECONDS));
        assertThat(Status.fromThrowable(error).getCode()).isEqualTo(Status.Code.INTERNAL);
    }

    @Test
    void clientCancellationDoesNotCancelKafkaPublication() throws Exception {
        var pending = new CompletableFuture<Void>();
        returns(true, pending);
        var response = call(true);
        awaitPublication(true);
        assertThat(response.cancel(true)).isTrue();
        assertThat(pending.isCancelled()).isFalse();
        pending.complete(null);
        assertThat(response.isCancelled()).isTrue();
        returns(false, CompletableFuture.completedFuture(null));
        assertThat(call(false).get(3, TimeUnit.SECONDS)).isEqualTo(Empty.getDefaultInstance());
    }

    @Test
    @SuppressWarnings("unchecked")
    void lateKafkaCallbackDoesNotWriteAfterCancellation() {
        var pending = new CompletableFuture<Void>();
        returns(true, pending);
        var observer = (ServerCallStreamObserver<Empty>) mock(ServerCallStreamObserver.class);
        var cancellation = new AtomicReference<Runnable>();
        doAnswer(invocation -> {
            cancellation.set(invocation.getArgument(0));
            return null;
        }).when(observer).setOnCancelHandler(any());
        new GrpcEventController(dispatcher, collector).collectSensorEvent(sensor(), observer);
        cancellation.get().run();
        pending.completeExceptionally(new PublicationException(new RuntimeException("late failure")));
        verify(observer, never()).onNext(any());
        verify(observer, never()).onCompleted();
        verify(observer, never()).onError(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void internalMappingFailureCompletesObserverOnceWithoutPublishing() {
        var brokenDispatcher = mock(EventDispatcher.class);
        when(brokenDispatcher.map(any(SensorEventProto.class))).thenThrow(new IllegalStateException("mapping failure"));
        var observer = (io.grpc.stub.StreamObserver<Empty>) mock(io.grpc.stub.StreamObserver.class);
        new GrpcEventController(brokenDispatcher, collector).collectSensorEvent(sensor(), observer);
        var error = org.mockito.ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(error.capture());
        assertThat(Status.fromThrowable(error.getValue()).getCode()).isEqualTo(Status.Code.INTERNAL);
        verifyNoMoreInteractions(observer);
        verifyNoInteractions(collector);
    }

}
