package ru.yandex.practicum.analyzer.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.practicum.analyzer.config.KafkaSettings;

/** Owns a consumer; only wakeup is called from another thread while the loop is running. */
public final class KafkaLoop<T> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaLoop.class);
    private final Consumer<String, T> consumer;
    private final KafkaSettings settings;
    private final String topic;
    private final java.util.function.Consumer<T> handler;
    private final Function<T, String> hubId;
    private final CountDownLatch finished = new CountDownLatch(1);
    private volatile boolean stopping;
    private boolean started;

    public KafkaLoop(Consumer<String, T> consumer, KafkaSettings settings, String topic,
              java.util.function.Consumer<T> handler, Function<T, String> hubId) {
        this.consumer = consumer;
        this.settings = settings;
        this.topic = topic;
        this.handler = handler;
        this.hubId = hubId;
    }

    public void run() {
        synchronized (this) {
            if (started) {
                if (stopping) {
                    return;
                }
                throw new IllegalStateException("Consumer loop already started");
            }
            started = true;
        }
        RuntimeException failure = null;
        try {
            log.info("Starting consumer topic={}", topic);
            consumer.subscribe(List.of(topic));
            while (!stopping) {
                var records = consumer.poll(settings.pollTimeout());
                for (var record : records) {
                    if (stopping) {
                        break;
                    }
                    try {
                        handler.accept(record.value());
                        var offset = Map.of(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                        try {
                            consumer.commitSync(offset);
                        } catch (WakeupException e) {
                            if (!stopping) {
                                throw e;
                            }
                            // Only this successfully processed record is safe to commit on shutdown.
                            consumer.commitSync(offset);
                        }
                    } catch (RuntimeException e) {
                        log.error("Processing failed topic={} partition={} offset={} hub={}",
                                record.topic(), record.partition(), record.offset(),
                                record.value() == null ? null : hubId.apply(record.value()), e);
                        throw e;
                    }
                }
            }
        } catch (WakeupException e) {
            if (!stopping) {
                failure = e;
            }
        } catch (RuntimeException e) {
            if (e instanceof RecordDeserializationException invalid) {
                log.error("Decode failed topic={} partition={} offset={}", invalid.topicPartition().topic(),
                        invalid.topicPartition().partition(), invalid.offset(), e);
            }
            failure = e;
        } finally {
            try {
                consumer.close(settings.closeTimeout());
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                log.info("Consumer closed topic={}", topic);
                finished.countDown();
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    public synchronized void requestStop() {
        if (!stopping) {
            stopping = true;
            if (started && finished.getCount() != 0) {
                consumer.wakeup();
            }
        }
    }

    @Override
    public void close() {
        requestStop();
        boolean unused;
        synchronized (this) {
            unused = !started;
            if (unused) {
                started = true;
            }
        }
        if (unused) {
            try {
                consumer.close(settings.closeTimeout());
            } finally {
                finished.countDown();
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
