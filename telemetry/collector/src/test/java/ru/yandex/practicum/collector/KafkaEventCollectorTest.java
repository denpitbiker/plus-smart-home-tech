package ru.yandex.practicum.collector;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.collector.config.KafkaSettings;
import ru.yandex.practicum.collector.kafka.PublicationException;
import ru.yandex.practicum.collector.service.KafkaEventCollector;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.serialization.AvroSerializer;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaEventCollectorTest {
    private KafkaEventCollector collector(Producer<String, SpecificRecord> producer) {
        return new KafkaEventCollector(producer,
                new KafkaSettings("unused", "sensors", "hubs", Map.of(), Duration.ofSeconds(10)));
    }

    private SensorEventAvro sensor() {
        return new SensorEventAvro("switch", "hub-1", Instant.ofEpochMilli(123456), new SwitchSensorAvro(false));
    }

    @Test
    void routesAndWaitsForBroker() {
        try (var producer = new MockProducer<String, SpecificRecord>(false, new StringSerializer(), new AvroSerializer())) {
            var service = collector(producer);
            var sensor = sensor();
            var result = service.collect(sensor);
            assertThat(result).isNotDone();
            var sent = producer.history().getFirst();
            assertThat(sent.topic()).isEqualTo("sensors");
            assertThat(sent.key()).isEqualTo("hub-1");
            assertThat(sent.timestamp()).isEqualTo(123456);
            assertThat(sent.value()).isSameAs(sensor);
            producer.completeNext();
            assertThat(result).isCompleted();
            var hub = new HubEventAvro("hub-2", Instant.ofEpochMilli(654321), new DeviceRemovedEventAvro("switch"));
            var failed = service.collect(hub);
            var hubRecord = producer.history().get(1);
            assertThat(hubRecord.topic()).isEqualTo("hubs");
            assertThat(hubRecord.key()).isEqualTo("hub-2");
            assertThat(hubRecord.timestamp()).isEqualTo(654321);
            assertThat(hubRecord.value()).isSameAs(hub);
            producer.errorNext(new TimeoutException("offline"));
            assertThatThrownBy(failed::join).hasCauseInstanceOf(PublicationException.class);
        }
    }

    @Test
    void callbackSerializationErrorIsNotWrappedAsUnavailable() {
        try (var producer = new MockProducer<String, SpecificRecord>(false, new StringSerializer(), new AvroSerializer())) {
            var result = collector(producer).collect(sensor());
            producer.errorNext(new SerializationException("encoding failure"));
            assertThatThrownBy(result::join).hasCauseInstanceOf(SerializationException.class);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void synchronousSendFailureCompletesFutureWithoutFlushing() {
        var producer = (Producer<String, SpecificRecord>) mock(Producer.class);
        when(producer.send(any(ProducerRecord.class), any())).thenThrow(new TimeoutException("metadata timeout"));
        var result = collector(producer).collect(sensor());
        assertThatThrownBy(result::join).hasCauseInstanceOf(PublicationException.class);
        verify(producer, never()).flush();
    }

    @Test
    @SuppressWarnings("unchecked")
    void synchronousSerializationFailurePreservesItsType() {
        var producer = (Producer<String, SpecificRecord>) mock(Producer.class);
        when(producer.send(any(ProducerRecord.class), any())).thenThrow(new SerializationException("encoding failure"));
        var result = collector(producer).collect(sensor());
        assertThatThrownBy(result::join).hasCauseInstanceOf(SerializationException.class);
    }
}
