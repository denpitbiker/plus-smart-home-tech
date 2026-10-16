package ru.yandex.practicum.aggregator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.aggregator.config.KafkaSettings;
import ru.yandex.practicum.aggregator.state.SnapshotStore;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.serialization.AvroSerializer;

import static org.junit.jupiter.api.Assertions.*;

class AggregationStarterTest {
    private static final TopicPartition PARTITION = new TopicPartition("sensors", 0);
    private static final Instant TIME = Instant.parse("2024-08-06T15:11:24.157Z");
    private final List<String> calls = new ArrayList<>();
    private final TestConsumer consumer = new TestConsumer();
    private final TestProducer producer = new TestProducer();
    private final KafkaSettings settings = new KafkaSettings("unused:9092", "sensors", "snapshots", "test",
            Map.of(), Map.of(), Duration.ofMillis(10), Duration.ofSeconds(2));
    private final AggregationStarter starter = new AggregationStarter(consumer, producer, new SnapshotStore(), settings);

    private class TestConsumer extends MockConsumer<String, SensorEventAvro> {
        private Map<TopicPartition, OffsetAndMetadata> committedOffsets = Map.of();
        private RuntimeException commitFailure;
        private boolean failClose;
        private boolean wakeCommit;
        private int closeCount;
        private final CountDownLatch woken = new CountDownLatch(1);

        TestConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public synchronized void wakeup() {
            super.wakeup();
            woken.countDown();
        }

        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            calls.add("commit");
            if (wakeCommit) {
                wakeCommit = false;
                throw new WakeupException();
            }
            if (commitFailure != null) {
                throw commitFailure;
            }
            committedOffsets = Map.copyOf(offsets);
            super.commitSync(offsets);
        }

        @Override
        public void close(Duration timeout) {
            assertEquals(settings.closeTimeout(), timeout);
            calls.add("consumer-close");
            closeCount++;
            super.close(timeout);
            if (failClose) {
                throw new IllegalStateException("consumer close failed");
            }
        }
    }

    private class TestProducer extends MockProducer<String, SpecificRecord> {
        private RuntimeException sendFailure;
        private boolean failFlush;
        private int closeCount;
        private CountDownLatch flushing;
        private CountDownLatch release;

        TestProducer() {
            super(false, new StringSerializer(), new AvroSerializer());
        }

        @Override
        public void flush() {
            calls.add("flush");
            if (flushing != null) {
                flushing.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            if (failFlush) {
                throw new IllegalStateException("flush failed");
            }
            if (sendFailure != null) {
                errorNext(sendFailure);
            }
            while (completeNext()) {
                // Complete all callbacks before returning, like KafkaProducer.flush().
            }
            super.flush();
        }

        @Override
        public void close(Duration timeout) {
            assertEquals(settings.closeTimeout(), timeout);
            calls.add("producer-close");
            closeCount++;
            super.close(timeout);
        }
    }

    private SensorEventAvro event(String hub, long millis, boolean state) {
        return new SensorEventAvro("sensor", hub, TIME.plusMillis(millis), new SwitchSensorAvro(state));
    }

    private void batch(SensorEventAvro... events) {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(PARTITION));
            consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
            for (int i = 0; i < events.length; i++) {
                consumer.addRecord(new ConsumerRecord<>("sensors", 0, i, "hub", events[i]));
            }
        });
        // End deterministic test runs after one batch; unexpected wakeups are fatal.
        consumer.schedulePollTask(() -> { throw new WakeupException(); });
    }

    @Test
    void publishesChangesAndCommitsAfterAcknowledgementsIncludingIgnoredRecords() {
        batch(event("hub", 0, false), event("hub", 1, false), event("hub", -1, true),
                event("hub", 2, true), event("other-hub", 0, false));
        assertThrows(WakeupException.class, starter::start);
        assertEquals(3, producer.history().size());
        var first = producer.history().getFirst();
        assertEquals("snapshots", first.topic());
        assertEquals("hub", first.key());
        assertEquals(TIME.toEpochMilli(), first.timestamp());
        assertEquals(new SwitchSensorAvro(false),
                ((SensorsSnapshotAvro) first.value()).getSensorsState().get("sensor").getData());
        assertEquals("other-hub", producer.history().getLast().key());
        assertEquals(5, consumer.committedOffsets.get(PARTITION).offset());
        assertEquals(List.of("flush", "commit", "flush", "consumer-close", "producer-close"), calls);
        starter.close();
        assertEquals(1, producer.closeCount);
        assertEquals(1, consumer.closeCount);
    }

    @Test
    void callbackFailureDoesNotCommitBatch() {
        producer.sendFailure = new IllegalStateException("broker unavailable");
        batch(event("hub", 0, false), event("hub", 1, true));
        var failure = assertThrows(IllegalStateException.class, starter::start);
        assertSame(producer.sendFailure, failure.getCause());
        assertTrue(consumer.committedOffsets.isEmpty());
        assertFalse(calls.contains("commit"));
        assertTrue(consumer.closed());
        assertTrue(producer.closed());
    }

    @Test
    void synchronousSendFailureDoesNotCommit() {
        producer.sendException = new SerializationException("serialization failed");
        batch(event("hub", 0, false));
        assertThrows(SerializationException.class, starter::start);
        assertFalse(calls.contains("commit"));
        assertTrue(producer.closed());
    }

    @Test
    void invalidEventAfterValidEventDoesNotCommitPartialBatch() {
        batch(event("hub", 0, false), null);
        assertThrows(IllegalArgumentException.class, starter::start);
        assertEquals(1, producer.history().size());
        assertFalse(calls.contains("commit"));
        assertTrue(consumer.closed());
        assertTrue(producer.closed());
    }

    @Test
    void deserializationFailureFromPollStopsWithoutCommit() {
        consumer.schedulePollTask(() -> { throw new SerializationException("corrupt Avro"); });
        assertThrows(SerializationException.class, starter::start);
        assertFalse(calls.contains("commit"));
        assertTrue(consumer.closed());
        assertTrue(producer.closed());
    }

    @Test
    void commitFailureStopsAndDoesNotRetryOrCommitInFinally() {
        consumer.commitFailure = new IllegalStateException("commit failed");
        batch(event("hub", 0, false));
        assertSame(consumer.commitFailure, assertThrows(IllegalStateException.class, starter::start));
        assertEquals(1, calls.stream().filter("commit"::equals).count());
        assertTrue(producer.closed());
    }

    @Test
    void closesBothClientsEvenIfFlushAndConsumerCloseFail() {
        producer.failFlush = true;
        consumer.failClose = true;
        batch(event("hub", 0, false));
        var failure = assertThrows(IllegalStateException.class, starter::start);
        assertEquals("flush failed", failure.getMessage());
        assertEquals(2, failure.getSuppressed().length);
        assertFalse(calls.contains("commit"));
        assertEquals(1, consumer.closeCount);
        assertEquals(1, producer.closeCount);
    }

    @Test
    void shutdownDuringFlushWaitsForAckAndCommitsCompletedBatch() throws Exception {
        consumer.wakeCommit = true;
        producer.flushing = new CountDownLatch(1);
        producer.release = new CountDownLatch(1);
        batch(event("hub", 0, false));
        try (var executor = Executors.newFixedThreadPool(2)) {
            var run = executor.submit(starter::start);
            assertTrue(producer.flushing.await(5, TimeUnit.SECONDS));
            assertTrue(consumer.committedOffsets.isEmpty());
            var close = executor.submit(starter::close);
            // Ensure close has requested wakeup before completing the publication.
            assertTrue(consumer.woken.await(5, TimeUnit.SECONDS));
            assertFalse(close.isDone());
            producer.release.countDown();
            run.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
        } finally {
            producer.release.countDown();
        }
        assertEquals(1, consumer.committedOffsets.get(PARTITION).offset());
        assertEquals(1, producer.closeCount);
        assertEquals(1, consumer.closeCount);
    }

    @Test
    void commitsExplicitNextOffsetsForEachPartition() {
        var second = new TopicPartition("sensors", 1);
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(PARTITION, second));
            consumer.updateBeginningOffsets(Map.of(PARTITION, 5L, second, 10L));
            consumer.addRecord(new ConsumerRecord<>("sensors", 0, 5, "hub", event("hub", 0, false)));
            consumer.addRecord(new ConsumerRecord<>("sensors", 1, 10, "other", event("other", 0, true)));
        });
        consumer.schedulePollTask(() -> { throw new WakeupException(); });
        assertThrows(WakeupException.class, starter::start);
        assertEquals(6, consumer.committedOffsets.get(PARTITION).offset());
        assertEquals(11, consumer.committedOffsets.get(second).offset());
    }

    @Test
    void springContextClosesClientsOnlyThroughStarter() {
        try (var context = new org.springframework.context.annotation.AnnotationConfigApplicationContext()) {
            context.registerBean("consumer", TestConsumer.class, () -> consumer,
                    definition -> definition.setDestroyMethodName(""));
            context.registerBean("producer", TestProducer.class, () -> producer,
                    definition -> definition.setDestroyMethodName(""));
            context.registerBean(AggregationStarter.class, () -> starter);
            context.refresh();
        }
        assertEquals(1, consumer.closeCount);
        assertEquals(1, producer.closeCount);
    }

    @Test
    void closesOnceWhenContextStopsBeforeLoopStarts() {
        starter.close();
        starter.close();
        assertEquals(List.of("flush", "consumer-close", "producer-close"), calls);
        assertThrows(IllegalStateException.class, starter::start);
    }
}
