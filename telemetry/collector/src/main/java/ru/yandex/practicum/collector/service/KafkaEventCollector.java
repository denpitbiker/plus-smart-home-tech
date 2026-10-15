package ru.yandex.practicum.collector.service;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.config.KafkaSettings;
import ru.yandex.practicum.collector.dto.*;
import ru.yandex.practicum.collector.kafka.PublicationException;
import ru.yandex.practicum.collector.mapping.*;
@Service
public class KafkaEventCollector implements EventCollector {
    private static final Logger log = LoggerFactory.getLogger(KafkaEventCollector.class);
    private final Producer<String, SpecificRecord> producer;
    private final KafkaSettings settings;
    private final SensorEventMapper sensors;
    private final HubEventMapper hubs;

    public KafkaEventCollector(Producer<String, SpecificRecord> producer, KafkaSettings settings,
                               SensorEventMapper sensors, HubEventMapper hubs) {
        this.producer = producer; this.settings = settings; this.sensors = sensors; this.hubs = hubs;
    }

    @Override public CompletableFuture<Void> collect(SensorEvent e) {
        return send(settings.sensorsTopic(), e.hubId, e.timestamp, e.getType(), sensors.map(e));
    }

    @Override public CompletableFuture<Void> collect(HubEvent e) {
        return send(settings.hubsTopic(), e.hubId, e.timestamp, e.getType(), hubs.map(e));
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
}
