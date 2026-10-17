package ru.yandex.practicum.analyzer;

import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.serialization.AvroSerializer;

import static org.junit.jupiter.api.Assertions.*;

/** Packaged Analyzer with real Kafka/PostgreSQL and a gRPC Hub Router test server. */
class AnalyzerIT {
    private static final Instant TIME = Instant.parse("2024-08-06T15:11:24.157Z");
    private final String bootstrap = System.getProperty("test.kafka.bootstrap", "localhost:9092");
    private final String dbUrl = System.getProperty("test.postgres.url", "jdbc:postgresql://localhost:5432/telemetry_analyzer");
    private final String user = System.getProperty("test.postgres.username", "dbuser");
    private final String password = System.getProperty("test.postgres.password", "12345");
    private final String id = UUID.randomUUID().toString().replace("-", "");
    private final String schema = "analyzer_it_" + id;
    private final String hubs = "analyzer-it-" + id + "-hubs";
    private final String snapshots = "analyzer-it-" + id + "-snapshots";
    private final String hubGroup = hubs + "-group";
    private final String snapshotGroup = snapshots + "-group";
    private final List<App> apps = new ArrayList<>();
    private final List<DeviceActionRequest> received = new CopyOnWriteArrayList<>();
    private final AtomicReference<BiConsumer<DeviceActionRequest, StreamObserver<Empty>>> response =
            new AtomicReference<>((request, observer) -> acknowledge(observer));
    private Admin admin;
    private Server router;
    private int routerPort;
    private long hubOffset;
    private long snapshotOffset;

    @BeforeEach
    void setup() throws Exception {
        sql("CREATE SCHEMA " + schema);
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap));
        admin.createTopics(List.of(new NewTopic(hubs, 1, (short) 1), new NewTopic(snapshots, 1, (short) 1)))
                .all().get(20, TimeUnit.SECONDS);
        router = ServerBuilder.forPort(0).addService(new HubRouterControllerGrpc.HubRouterControllerImplBase() {
            @Override
            public void handleDeviceAction(DeviceActionRequest request, StreamObserver<Empty> observer) {
                received.add(request);
                response.get().accept(request, observer);
            }
        }).build().start();
        routerPort = router.getPort();
    }

    @AfterEach
    void cleanup() throws Exception {
        try {
            for (var app : apps) {
                app.close();
            }
        } finally {
            try {
                if (router != null) {
                    router.shutdownNow();
                    assertTrue(router.awaitTermination(5, TimeUnit.SECONDS));
                }
            } finally {
                try {
                    if (admin != null) {
                        admin.deleteTopics(List.of(hubs, snapshots)).all().get(20, TimeUnit.SECONDS);
                        // A startup failure may never have created either group.
                        for (String group : List.of(hubGroup, snapshotGroup)) {
                            try {
                                admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
                            } catch (java.util.concurrent.ExecutionException e) {
                                if (!(e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException)) {
                                    throw e;
                                }
                            }
                        }
                    }
                } finally {
                    if (admin != null) {
                        admin.close(Duration.ofSeconds(5));
                    }
                    sql("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
                }
            }
        }
    }

    private static void acknowledge(StreamObserver<Empty> observer) {
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    private void sql(String query) throws Exception {
        try (var connection = DriverManager.getConnection(dbUrl, user, password);
             var statement = connection.createStatement()) {
            statement.execute(query);
        }
    }

    private void publish(String topic, String hub, SpecificRecord value) throws Exception {
        try (var producer = new KafkaProducer<String, SpecificRecord>(Map.of("bootstrap.servers", bootstrap,
                "acks", "all"), new StringSerializer(), new AvroSerializer())) {
            producer.send(new ProducerRecord<>(topic, hub, value)).get(10, TimeUnit.SECONDS);
        }
    }

    private void hub(String hub, Object payload) throws Exception {
        publish(hubs, hub, new HubEventAvro(hub, TIME, payload));
        hubOffset++;
    }

    private void snapshot(String hub, Map<String, SensorStateAvro> states) throws Exception {
        publish(snapshots, hub, new SensorsSnapshotAvro(hub, TIME, states));
        snapshotOffset++;
    }

    private long committed(String group, String topic) throws Exception {
        var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
        return offset == null ? 0 : offset.offset();
    }

    private void awaitOffset(String group, String topic, long expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (committed(group, topic) == expected) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Offset did not reach " + expected + ": " + topic + "; logs: " + apps.stream().map(app -> app.log).toList());
    }

    private void awaitHub() throws Exception {
        awaitOffset(hubGroup, hubs, hubOffset);
    }

    private void awaitSnapshots() throws Exception {
        awaitOffset(snapshotGroup, snapshots, snapshotOffset);
    }

    private void register(String hub, String... ids) throws Exception {
        for (String sensor : ids) {
            hub(hub, new DeviceAddedEventAvro(sensor, DeviceTypeAvro.SWITCH_SENSOR));
        }
    }

    private ScenarioAddedEventAvro scenario(String name, List<ScenarioConditionAvro> conditions,
                                            List<DeviceActionAvro> actions) {
        return new ScenarioAddedEventAvro(name, conditions, actions);
    }

    private ScenarioAddedEventAvro simpleScenario() {
        return scenario("scenario", List.of(condition("switch", ConditionTypeAvro.SWITCH, true)),
                List.of(new DeviceActionAvro("target", ActionTypeAvro.ACTIVATE, null),
                        new DeviceActionAvro("target", ActionTypeAvro.SET_VALUE, 0)));
    }

    private ScenarioConditionAvro condition(String sensor, ConditionTypeAvro type, Object value) {
        return new ScenarioConditionAvro(sensor, type, ConditionOperationAvro.EQUALS, value);
    }

    private SensorStateAvro state(Object value) {
        return new SensorStateAvro(TIME, value);
    }

    private App start(String... overrides) throws Exception {
        var app = new App(overrides);
        apps.add(app);
        return app;
    }

    private class App implements AutoCloseable {
        final Path log;
        final Process process;
        boolean closed;

        App(String... overrides) throws Exception {
            log = Path.of("target", "analyzer-it-" + id + "-" + apps.size() + ".log").toAbsolutePath();
            var command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-jar", Path.of("target/analyzer-1.0-SNAPSHOT.jar").toAbsolutePath().toString(),
                    "--spring.datasource.url=" + dbUrl,
                    "--spring.datasource.username=" + user,
                    "--spring.datasource.password=" + password,
                    "--spring.datasource.hikari.schema=" + schema,
                    "--spring.datasource.hikari.connection-timeout=1000",
                    "--spring.flyway.default-schema=" + schema,
                    "--spring.flyway.schemas=" + schema,
                    "--spring.jpa.properties.hibernate.default_schema=" + schema,
                    "--analyzer.kafka.bootstrap-servers=" + bootstrap,
                    "--analyzer.kafka.hubs-topic=" + hubs,
                    "--analyzer.kafka.snapshots-topic=" + snapshots,
                    "--analyzer.kafka.hubs-group-id=" + hubGroup,
                    "--analyzer.kafka.snapshots-group-id=" + snapshotGroup,
                    "--analyzer.kafka.poll-timeout=100ms",
                    "--analyzer.kafka.close-timeout=3s",
                    "--analyzer.hub-router.deadline=2s",
                    "--grpc.client.hub-router.address=static://localhost:" + routerPort));
            // Replace defaults rather than passing duplicate command-line properties.
            for (String override : overrides) {
                String key = override.substring(0, override.indexOf('=') + 1);
                command.removeIf(argument -> argument.startsWith(key));
                command.add(override);
            }
            process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        }

        void failed(String diagnostic) throws Exception {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), () -> "Process did not fail: " + log);
            assertNotEquals(0, process.exitValue(), () -> "Unexpected success: " + log);
            assertTrue(Files.readString(log).contains(diagnostic), () -> "Missing " + diagnostic + ": " + log);
        }

        @Override
        public void close() throws Exception {
            if (closed) {
                return;
            }
            closed = true;
            if (process.isAlive()) {
                process.destroy();
                if (!process.waitFor(20, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                    fail("Analyzer did not stop gracefully: " + log);
                }
            }
            String text = Files.readString(log);
            if (text.contains("Starting consumer topic=")) {
                assertTrue(text.contains("Consumer closed topic=" + hubs), () -> "Hub consumer not closed: " + log);
                assertTrue(text.contains("Consumer closed topic=" + snapshots), () -> "Snapshot consumer not closed: " + log);
            }
            if (text.contains("HikariPool-1 - Start completed.")) {
                assertTrue(text.contains("HikariPool-1 - Shutdown completed."), () -> "Pool not closed: " + log);
            }
        }
    }

    @Test
    void evaluatesAllReadingsAndActionsAcrossHubsAndAppliesScenarioChanges() throws Exception {
        try (var app = start()) {
            register("hub-a", "motion", "light", "switch", "climate", "temperature", "target");
            register("hub-b", "other-switch", "other-target");
            register("hub-a", "target"); // duplicate registration
            var conditions = List.of(
                    condition("motion", ConditionTypeAvro.MOTION, true),
                    new ScenarioConditionAvro("light", ConditionTypeAvro.LUMINOSITY, ConditionOperationAvro.LOWER_THAN, 20),
                    condition("switch", ConditionTypeAvro.SWITCH, false),
                    new ScenarioConditionAvro("temperature", ConditionTypeAvro.TEMPERATURE, ConditionOperationAvro.GREATER_THAN, 19),
                    condition("climate", ConditionTypeAvro.TEMPERATURE, 20),
                    condition("climate", ConditionTypeAvro.CO2LEVEL, 400),
                    condition("climate", ConditionTypeAvro.HUMIDITY, 50));
            var actions = java.util.Arrays.stream(ActionTypeAvro.values())
                    .map(type -> new DeviceActionAvro("target", type, type == ActionTypeAvro.SET_VALUE ? 0 : null)).toList();
            var scene = scenario("all-readings", conditions, actions);
            hub("hub-a", scene);
            hub("hub-a", scene); // replacement must not duplicate commands
            hub("hub-b", scenario("other-scene", List.of(condition("other-switch", ConditionTypeAvro.SWITCH, true)),
                    List.of(new DeviceActionAvro("other-target", ActionTypeAvro.INVERSE, null))));
            awaitHub();
            var states = Map.of(
                    "motion", state(new MotionSensorAvro(75, true, 220)),
                    "light", state(new LightSensorAvro(75, 10)),
                    "switch", state(new SwitchSensorAvro(false)),
                    "climate", state(new ClimateSensorAvro(20, 50, 400)),
                    "temperature", state(new TemperatureSensorAvro("temperature", "hub-a", TIME, 20, 68)));
            snapshot("hub-a", states);
            snapshot("hub-b", Map.of("other-switch", state(new SwitchSensorAvro(true))));
            awaitSnapshots();
            assertEquals(5, received.size());
            for (int i = 0; i < 4; i++) {
                var command = received.get(i);
                assertEquals("hub-a", command.getHubId());
                assertEquals("all-readings", command.getScenarioName());
                assertEquals("target", command.getAction().getSensorId());
                assertEquals(actions.get(i).getType().name(), command.getAction().getType().name());
                assertEquals(i == 3, command.getAction().hasValue());
                assertEquals(0, command.getAction().getValue());
                assertEquals(TIME.getEpochSecond(), command.getTimestamp().getSeconds());
                assertEquals(TIME.getNano(), command.getTimestamp().getNanos());
            }
            assertEquals("hub-b", received.get(4).getHubId());
            hub("hub-a", scenario("all-readings", conditions,
                    List.of(new DeviceActionAvro("target", ActionTypeAvro.DEACTIVATE, null))));
            awaitHub();
            snapshot("hub-a", states);
            awaitSnapshots();
            assertEquals(6, received.size());
            assertEquals("DEACTIVATE", received.getLast().getAction().getType().name());
            hub("hub-a", new DeviceRemovedEventAvro("motion"));
            hub("hub-a", new DeviceRemovedEventAvro("motion"));
            hub("hub-b", new ScenarioRemovedEventAvro("other-scene"));
            hub("hub-b", new ScenarioRemovedEventAvro("other-scene"));
            awaitHub();
            snapshot("hub-a", states);
            snapshot("hub-b", Map.of("other-switch", state(new SwitchSensorAvro(true))));
            awaitSnapshots();
            assertEquals(6, received.size());
        }
    }

    @Test
    void restartKeepsConfigurationAndResumesCommittedOffsets() throws Exception {
        try (var first = start()) {
            register("hub", "switch", "target");
            hub("hub", simpleScenario());
            awaitHub();
            snapshot("hub", Map.of("switch", state(new SwitchSensorAvro(true))));
            awaitSnapshots();
            assertEquals(2, received.size());
        }
        try (var second = start()) {
            snapshot("hub", Map.of("switch", state(new SwitchSensorAvro(true))));
            awaitSnapshots();
            assertEquals(4, received.size());
            assertEquals(hubOffset, committed(hubGroup, hubs));
        }
    }

    @Test
    void failedRpcLeavesSnapshotForReplayIncludingPreviouslySuccessfulActions() throws Exception {
        response.set((request, observer) -> {
            if (request.getAction().hasValue()) {
                observer.onError(Status.UNAVAILABLE.withDescription("Injected failure").asRuntimeException());
            } else {
                acknowledge(observer);
            }
        });
        try (var failed = start()) {
            register("hub", "switch", "target");
            hub("hub", simpleScenario());
            awaitHub();
            snapshot("hub", Map.of("switch", state(new SwitchSensorAvro(true))));
            failed.failed("UNAVAILABLE");
            assertEquals(0, committed(snapshotGroup, snapshots));
            assertEquals(2, received.size());
        }
        response.set((request, observer) -> acknowledge(observer));
        try (var restarted = start()) {
            awaitSnapshots();
            assertEquals(4, received.size());
            assertEquals(received.get(0), received.get(2)); // successful action is replayed too
            assertEquals(received.get(1), received.get(3));
        }
    }

    @Test
    void unavailableHubRouterFailsBothLoopsWithoutCommittingSnapshot() throws Exception {
        router.shutdownNow();
        assertTrue(router.awaitTermination(5, TimeUnit.SECONDS));
        try (var failed = start()) {
            register("hub", "switch", "target");
            hub("hub", simpleScenario());
            awaitHub();
            snapshot("hub", Map.of("switch", state(new SwitchSensorAvro(true))));
            failed.failed("UNAVAILABLE");
            assertEquals(0, committed(snapshotGroup, snapshots));
        }
    }

    @Test
    void unavailablePostgresPreventsStartupAndOffsetsRemainUncommitted() throws Exception {
        try (var unusedPort = new ServerSocket(0)) {
            int port = unusedPort.getLocalPort();
            unusedPort.close();
            try (var failed = start("--spring.datasource.url=jdbc:postgresql://127.0.0.1:" + port + "/missing?connectTimeout=1")) {
                failed.failed("Connection refused");
                assertEquals(0, committed(hubGroup, hubs));
                assertEquals(0, committed(snapshotGroup, snapshots));
            }
        }
    }

    @Test
    void pollTimeoutCausesCommitFailureAfterRpcAndStopsBothLoops() throws Exception {
        var pending = new AtomicReference<StreamObserver<Empty>>();
        response.set((request, observer) -> pending.set(observer));
        try (var failed = start("--analyzer.kafka.snapshot-properties[max.poll.interval.ms]=1000",
                "--analyzer.hub-router.deadline=20s")) {
            register("hub", "switch", "target");
            hub("hub", scenario("scenario", simpleScenario().getConditions(),
                    List.of(new DeviceActionAvro("target", ActionTypeAvro.ACTIVATE, null))));
            awaitHub();
            snapshot("hub", Map.of("switch", state(new SwitchSensorAvro(true))));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline
                    && (pending.get() == null || !Files.readString(failed.log).contains("consumer poll timeout has expired"))) {
                Thread.sleep(50);
            }
            assertNotNull(pending.get(), () -> "No pending RPC: " + failed.log);
            assertTrue(Files.readString(failed.log).contains("consumer poll timeout has expired"));
            acknowledge(pending.get());
            failed.failed("CommitFailedException");
            assertEquals(0, committed(snapshotGroup, snapshots));
            assertEquals(1, received.size());
        }
    }
}
