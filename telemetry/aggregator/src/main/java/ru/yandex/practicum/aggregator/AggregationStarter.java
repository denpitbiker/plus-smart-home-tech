package ru.yandex.practicum.aggregator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.config.KafkaSettings;
import ru.yandex.practicum.aggregator.state.SnapshotStore;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Component
public class AggregationStarter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AggregationStarter.class);
    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SpecificRecord> producer;
    private final SnapshotStore store;
    private final KafkaSettings settings;
    private final CountDownLatch finished = new CountDownLatch(1);
    private volatile boolean stopping;
    private boolean started;

    public AggregationStarter(Consumer<String, SensorEventAvro> consumer, Producer<String, SpecificRecord> producer,
                              SnapshotStore store, KafkaSettings settings) {
        this.consumer = consumer;
        this.producer = producer;
        this.store = store;
        this.settings = settings;
    }

    public void start() {
        synchronized (this) {
            if (started) {
                throw new IllegalStateException("Aggregation has already started or closed");
            }
            started = true;
        }
        RuntimeException failure = null;
        try {
            log.info("Starting aggregation from {} to {}", settings.sensorsTopic(), settings.snapshotsTopic());
            consumer.subscribe(List.of(settings.sensorsTopic()));
            while (!stopping) {
                processBatch(consumer.poll(settings.pollTimeout()));
            }
        } catch (WakeupException e) {
            if (!stopping) {
                failure = e;
            }
        } catch (RuntimeException e) {
            if (e instanceof RecordDeserializationException invalidRecord) {
                log.error("Cannot decode sensor event topic={} partition={} offset={}",
                        invalidRecord.topicPartition().topic(), invalidRecord.topicPartition().partition(),
                        invalidRecord.offset(), e);
            }
            failure = e;
        } finally {
            failure = finish(failure);
        }
        if (failure != null) {
            log.error("Aggregation stopped with an error", failure);
            throw failure;
        }
    }

    private void processBatch(ConsumerRecords<String, SensorEventAvro> records) {
        if (records.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        AtomicReference<Exception> sendFailure = new AtomicReference<>();
        for (var record : records) {
            try {
                var event = record.value();
                store.updateState(event).ifPresent(snapshot -> producer.send(
                        new ProducerRecord<>(settings.snapshotsTopic(), null, snapshot.getTimestamp().toEpochMilli(),
                                snapshot.getHubId(), snapshot),
                        (metadata, error) -> {
                            if (error != null) {
                                logRecordFailure(record, error);
                                sendFailure.compareAndSet(null, error);
                            }
                        }));
                offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
            } catch (RuntimeException e) {
                logRecordFailure(record, e);
                throw e;
            }
        }
        producer.flush();
        if (sendFailure.get() != null) {
            throw new IllegalStateException("Snapshot publication failed; batch offsets were not committed", sendFailure.get());
        }
        try {
            consumer.commitSync(offsets);
        } catch (WakeupException e) {
            if (!stopping) {
                throw e;
            }
            // A shutdown wakeup may reach commit instead of poll. This batch is fully acknowledged.
            consumer.commitSync(offsets);
        }
    }

    private void logRecordFailure(ConsumerRecord<String, SensorEventAvro> record, Exception failure) {
        var event = record.value();
        log.error("Failed sensor event topic={} partition={} offset={} hub={} sensor={}",
                record.topic(), record.partition(), record.offset(),
                event == null ? null : event.getHubId(), event == null ? null : event.getId(), failure);
    }

    private RuntimeException finish(RuntimeException failure) {
        try {
            producer.flush();
        } catch (RuntimeException e) {
            failure = append(failure, e);
        }
        try {
            consumer.close(settings.closeTimeout());
        } catch (RuntimeException e) {
            failure = append(failure, e);
        }
        try {
            producer.close(settings.closeTimeout());
        } catch (RuntimeException e) {
            failure = append(failure, e);
        } finally {
            log.info("Aggregation clients closed");
            finished.countDown();
        }
        return failure;
    }

    private RuntimeException append(RuntimeException failure, RuntimeException additional) {
        if (failure == null) {
            return additional;
        }
        if (failure != additional) {
            failure.addSuppressed(additional);
        }
        return failure;
    }

    @Override
    public void close() {
        boolean closeWithoutStart;
        synchronized (this) {
            closeWithoutStart = !started;
            if (closeWithoutStart) {
                started = true;
            }
            if (!stopping) {
                stopping = true;
                if (!closeWithoutStart && finished.getCount() != 0) {
                    consumer.wakeup();
                }
            }
        }
        if (closeWithoutStart) {
            var failure = finish(null);
            if (failure != null) {
                throw failure;
            }
        }
        boolean interrupted = false;
        while (true) {
            try {
                finished.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
