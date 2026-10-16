package ru.yandex.practicum.collector;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import ru.yandex.practicum.kafka.telemetry.event.*;
import static org.assertj.core.api.Assertions.*;
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CollectorIT {
    static final String BOOTSTRAP = System.getProperty("test.kafka.bootstrap", "localhost:9092");
    static final String PREFIX = "collector-it-" + UUID.randomUUID();
    static final String SENSORS = PREFIX + "-sensors", HUBS = PREFIX + "-hubs";

    @Autowired TestRestTemplate http;

    @DynamicPropertySource static void config(DynamicPropertyRegistry properties) {
        properties.add("collector.kafka.bootstrap-servers", () -> BOOTSTRAP);
        properties.add("collector.kafka.sensors-topic", () -> SENSORS);
        properties.add("collector.kafka.hubs-topic", () -> HUBS);
    }

    @Test void sendsAllNineEventsThroughHttpAndKafka() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            admin.createTopics(List.of(new NewTopic(SENSORS, 1, (short) 1), new NewTopic(HUBS, 1, (short) 1)))
                    .all().get(20, TimeUnit.SECONDS);
            try (var consumer = new KafkaConsumer<String, byte[]>(Map.of("bootstrap.servers", BOOTSTRAP,
                    "group.id", PREFIX, "enable.auto.commit", false), new StringDeserializer(), new ByteArrayDeserializer())) {
                var partitions = List.of(new TopicPartition(SENSORS, 0), new TopicPartition(HUBS, 0));
                consumer.assign(partitions); consumer.seekToBeginning(partitions);
                var headers = new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON);
                for (var event : EventFixtures.events()) {
                    var response = http.postForEntity("/events/" + event.path(), new HttpEntity<>(event.json(), headers), String.class);
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getBody()).isNullOrEmpty();
                }
                assertThat(http.postForEntity("/events/sensors", new HttpEntity<>("{}", headers), String.class).getStatusCode())
                        .isEqualTo(HttpStatus.BAD_REQUEST);
                var records = new ArrayList<ConsumerRecord<String, byte[]>>();
                long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                while (records.size() < 9 && System.nanoTime() < deadline) {
                    consumer.poll(Duration.ofMillis(250)).forEach(records::add);
                }
                assertThat(records).hasSize(9);
                for (String topic : List.of(SENSORS, HUBS)) {
                    var actual = records.stream().filter(e -> e.topic().equals(topic)).toList();
                    var expected = EventFixtures.events().stream().filter(e -> e.path().equals(topic.equals(SENSORS) ? "sensors" : "hubs")).toList();
                    assertThat(actual).hasSameSizeAs(expected);
                    for (int i = 0; i < actual.size(); i++) {
                        var message = actual.get(i);
                        var schema = topic.equals(SENSORS) ? SensorEventAvro.getClassSchema() : HubEventAvro.getClassSchema();
                        var decoder = DecoderFactory.get().binaryDecoder(message.value(), null);
                        var decoded = new SpecificDatumReader<SpecificRecord>(schema).read(null, decoder);
                        assertThat(decoded).isEqualTo(MappingTest.map(expected.get(i)));
                        assertThat(decoder.isEnd()).isTrue();
                        assertThat(message.key()).isEqualTo(topic.equals(SENSORS) ? "hub-1" : "hub-2");
                        assertThat(message.timestamp()).isEqualTo(1722957084157L);
                    }
                }
                assertThat(consumer.poll(Duration.ofMillis(500))).isEmpty();
            } finally {
                admin.deleteTopics(List.of(SENSORS, HUBS)).all().get(20, TimeUnit.SECONDS);
            }
        }
    }
}
