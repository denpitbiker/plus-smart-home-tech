package ru.yandex.practicum.analyzer.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.analyzer.config.KafkaSettings;

import static org.junit.jupiter.api.Assertions.*;

class KafkaLoopTest {
    static final KafkaSettings SETTINGS = new KafkaSettings("localhost:9092", "hubs", "snapshots",
            "hubs-group", "snapshots-group", Map.of(), Map.of(), Duration.ofMillis(10), Duration.ofSeconds(1));
    private static final TopicPartition PARTITION = new TopicPartition("hubs", 0);

    static class RecordingConsumer<T> extends MockConsumer<String, T> {
        final List<Map<TopicPartition, OffsetAndMetadata>> commits = new java.util.concurrent.CopyOnWriteArrayList<>();
        int closes;
        RuntimeException commitFailure;
        RuntimeException closeFailure;

        RecordingConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (commitFailure != null) {
                throw commitFailure;
            }
            super.commitSync(offsets);
            commits.add(Map.copyOf(offsets));
        }

        @Override
        public synchronized void close(Duration timeout) {
            closes++;
            super.close(timeout);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private RecordingConsumer<String> records() {
        var consumer = new RecordingConsumer<String>();
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(PARTITION));
            consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
            consumer.addRecord(new ConsumerRecord<>("hubs", 0, 0L, "hub", "first"));
            consumer.addRecord(new ConsumerRecord<>("hubs", 0, 1L, "hub", "second"));
        });
        return consumer;
    }

    @Test
    void commitsEachProcessedRecordAndClosesExactlyOnce() {
        var consumer = records();
        var received = new ArrayList<String>();
        var loop = new KafkaLoop<>(consumer, SETTINGS, "hubs", received::add, value -> "hub");
        consumer.schedulePollTask(loop::requestStop);
        loop.run();
        loop.close();
        loop.close();
        assertEquals(List.of("first", "second"), received);
        assertEquals(List.of(1L, 2L), consumer.commits.stream().map(map -> map.get(PARTITION).offset()).toList());
        assertEquals(1, consumer.closes);
    }

    @Test
    void doesNotCommitUntilHandlerCompletes() throws Exception {
        var consumer = records();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var loop = new KafkaLoop<>(consumer, SETTINGS, "hubs", value -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Handler was not released");
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }, value -> "hub");
        try (var executor = Executors.newSingleThreadExecutor()) {
            var task = executor.submit(loop::run);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertTrue(consumer.commits.isEmpty());
                loop.requestStop();
            } finally {
                release.countDown();
            }
            task.get(5, TimeUnit.SECONDS);
        }
        assertEquals(1, consumer.commits.size());
        assertEquals(1, consumer.commits.getFirst().get(PARTITION).offset());
        assertEquals(1, consumer.closes);
    }

    @Test
    void doesNotCommitFailedRecordOrLaterRecords() {
        var consumer = records();
        var loop = new KafkaLoop<>(consumer, SETTINGS, "hubs", value -> {
            if (value.equals("second")) {
                throw new IllegalStateException("Database or RPC failure");
            }
        }, value -> "hub");
        assertThrows(IllegalStateException.class, loop::run);
        assertEquals(1, consumer.commits.size());
        assertEquals(1, consumer.commits.getFirst().get(PARTITION).offset());
        assertEquals(1, consumer.closes);
    }

    @Test
    void propagatesCommitFailureAndStillCloses() {
        var consumer = records();
        consumer.commitFailure = new IllegalStateException("Commit failed");
        var loop = new KafkaLoop<>(consumer, SETTINGS, "hubs", value -> { }, value -> "hub");
        assertSame(consumer.commitFailure, assertThrows(IllegalStateException.class, loop::run));
        assertTrue(consumer.commits.isEmpty());
        assertEquals(1, consumer.closes);
    }

    @Test
    void propagatesDeserializationAndUnexpectedWakeup() {
        for (RuntimeException failure : List.of(
                new RecordDeserializationException(PARTITION, 3, "Corrupt data", new IllegalArgumentException()),
                new WakeupException())) {
            var consumer = new RecordingConsumer<String>();
            consumer.schedulePollTask(() -> { throw failure; });
            var loop = new KafkaLoop<>(consumer, SETTINGS, "hubs", value -> { }, value -> "hub");
            assertSame(failure, assertThrows(RuntimeException.class, loop::run));
            assertTrue(consumer.commits.isEmpty());
            assertEquals(1, consumer.closes);
        }
    }

    @Test
    void closesUnusedConsumerAndPreservesProcessingFailureWhenCloseFails() {
        var unused = new RecordingConsumer<String>();
        var unusedLoop = new KafkaLoop<>(unused, SETTINGS, "hubs", value -> { }, value -> "hub");
        unusedLoop.close();
        unusedLoop.close();
        assertEquals(1, unused.closes);
        var consumer = records();
        consumer.closeFailure = new IllegalStateException("close");
        var failure = new IllegalArgumentException("handler");
        var loop = new KafkaLoop<>(consumer, SETTINGS, "hubs", value -> { throw failure; }, value -> "hub");
        assertSame(failure, assertThrows(IllegalArgumentException.class, loop::run));
        assertArrayEquals(new Throwable[]{consumer.closeFailure}, failure.getSuppressed());
    }
}
