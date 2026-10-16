package ru.yandex.practicum.collector.service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.config.KafkaSettings;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.collector.kafka.PublicationException;

@Service
public class KafkaEventCollector implements EventCollector, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaEventCollector.class);
    private final Producer<String, SpecificRecord> producer;
    private final KafkaSettings settings;

    public KafkaEventCollector(Producer<String, SpecificRecord> producer, KafkaSettings settings) {
        this.producer = producer;
        this.settings = settings;
    }

    @Override
    public CompletableFuture<Void> collect(SensorEventAvro event) {
        return send(settings.sensorsTopic(), event.getHubId().toString(), event.getTimestamp(),
                event.getPayload().getClass().getSimpleName(), event);
    }

    @Override
    public CompletableFuture<Void> collect(HubEventAvro event) {
        return send(settings.hubsTopic(), event.getHubId().toString(), event.getTimestamp(),
                event.getPayload().getClass().getSimpleName(), event);
    }

    private CompletableFuture<Void> send(String topic, String hub, Instant time, String type, SpecificRecord data) {
        var result = new CompletableFuture<Void>();
        try {
            producer.send(new ProducerRecord<>(topic, null, time.toEpochMilli(), hub, data), (metadata, error) -> {
                if (error == null) result.complete(null);
                else fail(result, hub, type, error);
            });
        } catch (RuntimeException error) {
            fail(result, hub, type, error);
        }
        return result;
    }

    private void fail(CompletableFuture<Void> result, String hub, String type, Exception error) {
        log.error("Publication failed: hub={}, type={}", hub, type, error);
        result.completeExceptionally(error instanceof SerializationException ? error : new PublicationException(error));
    }

    @Override
    public void close() {
        try {
            producer.flush();
        } finally {
            producer.close(settings.closeTimeout());
        }
    }
}
