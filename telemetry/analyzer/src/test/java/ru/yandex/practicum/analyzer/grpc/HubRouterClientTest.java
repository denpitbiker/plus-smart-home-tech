package ru.yandex.practicum.analyzer.grpc;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import ru.yandex.practicum.analyzer.analysis.DeviceCommand;
import ru.yandex.practicum.analyzer.config.HubRouterSettings;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

import static org.junit.jupiter.api.Assertions.*;

class HubRouterClientTest {
    private Server server;
    private ManagedChannel channel;

    private HubRouterClient client(Duration deadline,
                                   BiConsumer<DeviceActionRequest, StreamObserver<Empty>> handler) throws Exception {
        server = ServerBuilder.forPort(0).addService(new HubRouterControllerGrpc.HubRouterControllerImplBase() {
            @Override
            public void handleDeviceAction(DeviceActionRequest request, StreamObserver<Empty> observer) {
                handler.accept(request, observer);
            }
        }).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        var client = new HubRouterClient(new HubRouterSettings(deadline));
        ReflectionTestUtils.setField(client, "stub", HubRouterControllerGrpc.newBlockingStub(channel));
        return client;
    }

    @AfterEach
    void close() throws Exception {
        if (channel != null) {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
        }
        if (server != null) {
            server.shutdownNow();
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void sendsAllActionsInOrderWithExactMetadataAndOptionalZero() throws Exception {
        var requests = new CopyOnWriteArrayList<DeviceActionRequest>();
        var client = client(Duration.ofSeconds(5), (request, observer) -> {
            requests.add(request);
            observer.onNext(Empty.getDefaultInstance());
            observer.onCompleted();
        });
        var time = Instant.parse("2024-08-06T15:11:24.157Z");
        var commands = java.util.Arrays.stream(ActionType.values())
                .map(type -> new DeviceCommand("hub", "scenario", "device", type,
                        type == ActionType.SET_VALUE ? 0 : null, time)).toList();
        client.send(commands);
        assertEquals(4, requests.size());
        for (int i = 0; i < requests.size(); i++) {
            var request = requests.get(i);
            assertEquals("hub", request.getHubId());
            assertEquals("scenario", request.getScenarioName());
            assertEquals("device", request.getAction().getSensorId());
            assertEquals(commands.get(i).type().name(), request.getAction().getType().name());
            assertEquals(i == 3, request.getAction().hasValue());
            assertEquals(0, request.getAction().getValue());
            assertEquals(time.getEpochSecond(), request.getTimestamp().getSeconds());
            assertEquals(time.getNano(), request.getTimestamp().getNanos());
        }
    }

    @Test
    void stopsAfterFirstFailedCommandWithoutRetrying() throws Exception {
        var received = new CopyOnWriteArrayList<String>();
        var client = client(Duration.ofSeconds(5), (request, observer) -> {
            received.add(request.getAction().getSensorId());
            if (received.size() == 2) {
                observer.onError(Status.UNAVAILABLE.asRuntimeException());
            } else {
                observer.onNext(Empty.getDefaultInstance());
                observer.onCompleted();
            }
        });
        var commands = List.of("first", "second", "third").stream().map(id ->
                new DeviceCommand("hub", "scenario", id, ActionType.INVERSE, null, Instant.EPOCH)).toList();
        var error = assertThrows(StatusRuntimeException.class, () -> client.send(commands));
        assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());
        assertEquals(List.of("first", "second"), received);
    }

    @Test
    void boundsUnansweredRpcWithDeadline() throws Exception {
        var client = client(Duration.ofMillis(200), (request, observer) -> { });
        var error = assertThrows(StatusRuntimeException.class, () -> client.send(List.of(
                new DeviceCommand("hub", "scenario", "sensor", ActionType.ACTIVATE, null, Instant.EPOCH))));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, error.getStatus().getCode());
    }
}
