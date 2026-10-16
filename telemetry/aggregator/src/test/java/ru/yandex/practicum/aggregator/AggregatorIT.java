package ru.yandex.practicum.aggregator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.kafka.telemetry.serialization.AvroSerializer;
import ru.yandex.practicum.kafka.telemetry.serialization.SensorsSnapshotDeserializer;

import static org.junit.jupiter.api.Assertions.*;

/** Real broker and packaged application processes; run with -Pintegration. */
class AggregatorIT {
    private static final String BOOTSTRAP = System.getProperty("test.kafka.bootstrap", "localhost:9092");
    private static final Instant TIME = Instant.parse("2024-08-06T15:11:24.157Z");
    private final String prefix = "aggregator-it-" + UUID.randomUUID();
    private final String sensors = prefix + "-sensors";
    private final String snapshots = prefix + "-snapshots";
    private final String group = prefix + "-group";
    private final List<App> apps = new ArrayList<>();
    private Admin admin;

    @BeforeEach
    void createTopics() throws Exception {
        admin = Admin.create(Map.of("bootstrap.servers", BOOTSTRAP));
        admin.createTopics(List.of(new NewTopic(sensors, 1, (short) 1), new NewTopic(snapshots, 1, (short) 1)))
                .all().get(20, TimeUnit.SECONDS);
    }

    @AfterEach
    void cleanup() throws Exception {
        try {
            for (var app : apps) {
                app.close();
            }
        } finally {
            if (admin != null) {
                try {
                    admin.deleteTopics(List.of(sensors, snapshots)).all().get(20, TimeUnit.SECONDS);
                    admin.deleteConsumerGroups(List.of(group)).all().get(20, TimeUnit.SECONDS);
                } finally {
                    admin.close(Duration.ofSeconds(5));
                }
            }
        }
    }

    private SensorEventAvro event(String hub, String id, long millis, Object payload) {
        return new SensorEventAvro(id, hub, TIME.plusMillis(millis), payload);
    }

    private void send(List<SensorEventAvro> events) throws Exception {
        try (var producer = new KafkaProducer<String, SpecificRecord>(Map.of("bootstrap.servers", BOOTSTRAP,
                "acks", "all"), new StringSerializer(), new AvroSerializer())) {
            for (var event : events) {
                producer.send(new ProducerRecord<>(sensors, event.getHubId(), event)).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private KafkaConsumer<String, SensorsSnapshotAvro> output() {
        var consumer = new KafkaConsumer<String, SensorsSnapshotAvro>(Map.of("bootstrap.servers", BOOTSTRAP,
                "enable.auto.commit", false), new StringDeserializer(), new SensorsSnapshotDeserializer());
        var partition = new TopicPartition(snapshots, 0);
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
        return consumer;
    }

    private List<ConsumerRecord<String, SensorsSnapshotAvro>> read(
            KafkaConsumer<String, SensorsSnapshotAvro> consumer, int count) {
        var records = new ArrayList<ConsumerRecord<String, SensorsSnapshotAvro>>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (records.size() < count && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
        }
        assertEquals(count, records.size());
        return records;
    }

    private void awaitOffset(long offset) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS).get(new TopicPartition(sensors, 0));
            if (committed != null && committed.offset() == offset) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Input offset did not reach " + offset);
    }

    private App start() throws Exception {
        var app = new App();
        apps.add(app);
        return app;
    }

    private class App implements AutoCloseable {
        private final Path log;
        private final Process process;
        private boolean closed;

        App() throws Exception {
            log = Path.of("target", prefix + "-" + apps.size() + ".log").toAbsolutePath();
            process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-jar", Path.of("target", "aggregator-1.0-SNAPSHOT.jar").toAbsolutePath().toString(),
                    "--aggregator.kafka.bootstrap-servers=" + BOOTSTRAP,
                    "--aggregator.kafka.sensors-topic=" + sensors,
                    "--aggregator.kafka.snapshots-topic=" + snapshots,
                    "--aggregator.kafka.group-id=" + group,
                    "--aggregator.kafka.producer-properties[compression.type]=none",
                    "--aggregator.kafka.close-timeout=3s")
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
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
                    fail("Application did not stop gracefully: " + log);
                }
            }
            assertTrue(Files.readString(log).contains("Aggregation clients closed"), () -> "No clean shutdown: " + log);
        }
    }

    @Test
    void aggregatesEveryPayloadAcrossHubsWithoutDuplicateSnapshots() throws Exception {
        var accepted = List.of(
                event("hub-1", "climate", 0, new ClimateSensorAvro(20, 50, 400)),
                event("hub-1", "light", 1, new LightSensorAvro(75, 10)),
                event("hub-1", "motion", 2, new MotionSensorAvro(90, false, 220)),
                event("hub-1", "switch", 3, new SwitchSensorAvro(false)),
                event("hub-1", "temperature", 4, new TemperatureSensorAvro("temperature", "hub-1", TIME.plusMillis(4), 21, 70)),
                event("hub-2", "switch", 0, new SwitchSensorAvro(true)),
                event("hub-1", "motion", 2, new MotionSensorAvro(90, true, 220)),
                event("hub-1", "light", 5, new LightSensorAvro(75, 11)));
        var input = new ArrayList<>(accepted);
        input.add(accepted.get(6)); // exact duplicate
        input.add(event("hub-1", "motion", 1, new MotionSensorAvro(90, false, 220))); // older
        input.add(event("hub-1", "light", 10, new LightSensorAvro(75, 11))); // unchanged
        send(input);
        try (var reader = output(); var app = start()) {
            var records = read(reader, accepted.size());
            Map<String, Map<String, SensorStateAvro>> expected = new LinkedHashMap<>();
            for (int i = 0; i < accepted.size(); i++) {
                var event = accepted.get(i);
                var states = expected.computeIfAbsent(event.getHubId(), ignored -> new LinkedHashMap<>());
                states.put(event.getId(), new SensorStateAvro(event.getTimestamp(), event.getPayload()));
                var snapshot = new SensorsSnapshotAvro(event.getHubId(), event.getTimestamp(), states);
                assertEquals(snapshot, records.get(i).value());
                assertEquals(event.getHubId(), records.get(i).key());
                assertEquals(event.getTimestamp().toEpochMilli(), records.get(i).timestamp());
            }
            var first = SpecificData.get().deepCopy(SensorsSnapshotAvro.getClassSchema(), records.getFirst().value());
            awaitOffset(input.size());
            assertTrue(reader.poll(Duration.ofMillis(500)).isEmpty());
            send(List.of(event("hub-1", "switch", 20, new SwitchSensorAvro(true))));
            var next = read(reader, 1).getFirst().value();
            assertEquals(5, next.getSensorsState().size());
            assertEquals(first, records.getFirst().value());
            awaitOffset(input.size() + 1);
        }
    }

    @Test
    void restartResumesOffsetsButDoesNotRestoreInMemorySensors() throws Exception {
        send(List.of(event("hub", "one", 0, new SwitchSensorAvro(false)),
                event("hub", "two", 1, new LightSensorAvro(50, 10))));
        try (var reader = output()) {
            try (var first = start()) {
                assertEquals(2, read(reader, 2).getLast().value().getSensorsState().size());
                awaitOffset(2);
            }
            send(List.of(event("hub", "one", 2, new SwitchSensorAvro(true))));
            try (var second = start()) {
                var snapshot = read(reader, 1).getFirst().value();
                assertEquals(Map.of("one", new SensorStateAvro(TIME.plusMillis(2), new SwitchSensorAvro(true))),
                        snapshot.getSensorsState());
                awaitOffset(3);
                assertTrue(reader.poll(Duration.ofMillis(500)).isEmpty());
            }
        }
    }

    @Test
    void publicationFailureLeavesOffsetsUncommittedAndRestartReplaysBatch() throws Exception {
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, snapshots);
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                new ConfigEntry("max.message.bytes", "256"), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS);
        // Input fits its topic, but the snapshot is rejected by the output topic.
        var oversized = event("hub", "sensor-" + "x".repeat(4096), 0, new SwitchSensorAvro(true));
        send(List.of(oversized));
        try (var failed = start()) {
            assertTrue(failed.process.waitFor(30, TimeUnit.SECONDS), () -> "No publication failure: " + failed.log);
            assertNotEquals(0, failed.process.exitValue());
            assertTrue(Files.readString(failed.log).contains("RecordTooLargeException"));
            assertTrue(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).isEmpty());
        }
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                new ConfigEntry("max.message.bytes", "1048576"), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS);
        try (var reader = output(); var restarted = start()) {
            var snapshot = read(reader, 1).getFirst().value();
            assertEquals(Map.of(oversized.getId(), new SensorStateAvro(TIME, oversized.getPayload())),
                    snapshot.getSensorsState());
            awaitOffset(1);
            assertTrue(reader.poll(Duration.ofMillis(500)).isEmpty());
        }
    }
}
