package ru.yandex.practicum.collector;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.collector.config.KafkaSettings;
import ru.yandex.practicum.collector.dto.*;
import ru.yandex.practicum.collector.kafka.*;
import ru.yandex.practicum.collector.mapping.*;
import ru.yandex.practicum.collector.service.KafkaEventCollector;
import static org.assertj.core.api.Assertions.*;
class KafkaEventCollectorTest {
    @Test void routesAndWaitsForBroker() throws Exception {
        try (var producer = new MockProducer<String, SpecificRecord>(false, new StringSerializer(), new AvroSerializer())) {
            var service = new KafkaEventCollector(producer, new KafkaSettings("unused", "sensors", "hubs"), new SensorEventMapper(), new HubEventMapper());
            var e = MappingTest.JSON.readValue(EventFixtures.events().getFirst().json(), SensorEvent.class);
            var result = service.collect(e);
            assertThat(result).isNotDone();
            var sent = producer.history().getFirst();
            assertThat(sent.topic()).isEqualTo("sensors"); assertThat(sent.key()).isEqualTo(e.hubId);
            assertThat(sent.timestamp()).isEqualTo(e.timestamp.toEpochMilli());
            producer.completeNext(); assertThat(result).isCompleted();
            var hub = MappingTest.JSON.readValue(EventFixtures.events().get(5).json(), HubEvent.class);
            var failed = service.collect(hub);
            assertThat(producer.history().get(1).topic()).isEqualTo("hubs");
            producer.errorNext(new org.apache.kafka.common.errors.TimeoutException("offline"));
            assertThatThrownBy(failed::join).hasCauseInstanceOf(PublicationException.class);
        }
    }
}
