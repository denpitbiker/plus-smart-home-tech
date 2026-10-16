package ru.yandex.practicum.analyzer.kafka;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.analyzer.analysis.SnapshotAnalysisService;
import ru.yandex.practicum.analyzer.grpc.HubRouterClient;
import ru.yandex.practicum.analyzer.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AnalysisRunnerTest {

    @Test
    void backgroundFailureStopsMainConsumerAndPropagates() throws Exception {
        var hubs = new KafkaLoopTest.RecordingConsumer<HubEventAvro>();
        var snapshots = new KafkaLoopTest.RecordingConsumer<SensorsSnapshotAvro>();
        var hubService = mock(HubEventService.class);
        var failure = new IllegalArgumentException("Database failed");
        doThrow(failure).when(hubService).handle(any());
        hubs.schedulePollTask(() -> {
            var partition = new TopicPartition("hubs", 0);
            hubs.rebalance(List.of(partition));
            hubs.updateBeginningOffsets(Map.of(partition, 0L));
            hubs.addRecord(new ConsumerRecord<>("hubs", 0, 0L, "hub", new HubEventAvro("hub", Instant.EPOCH, null)));
        });
        var runner = runner(hubs, snapshots, hubService);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var task = executor.submit(() -> assertThrows(IllegalStateException.class, runner::start));
            assertSame(failure, task.get(5, TimeUnit.SECONDS).getCause());
        }
        assertTrue(hubs.commits.isEmpty());
        assertTrue(snapshots.commits.isEmpty());
        assertEquals(1, hubs.closes);
        assertEquals(1, snapshots.closes);
        runner.close();
        assertEquals(1, hubs.closes);
    }

    @Test
    void shutdownWakesBothConsumersAndWaitsForTheirClosure() throws Exception {
        var hubs = new KafkaLoopTest.RecordingConsumer<HubEventAvro>();
        var snapshots = new KafkaLoopTest.RecordingConsumer<SensorsSnapshotAvro>();
        var polled = new CountDownLatch(2);
        hubs.schedulePollTask(polled::countDown);
        snapshots.schedulePollTask(polled::countDown);
        var runner = runner(hubs, snapshots, mock(HubEventService.class));
        try (var executor = Executors.newSingleThreadExecutor()) {
            var task = executor.submit(runner::start);
            try {
                assertTrue(polled.await(5, TimeUnit.SECONDS));
            } finally {
                runner.close();
            }
            task.get(5, TimeUnit.SECONDS);
        }
        assertEquals(1, hubs.closes);
        assertEquals(1, snapshots.closes);
    }

    @Test
    void closesSecondUnusedConsumerEvenIfFirstCloseFails() {
        var hubs = new KafkaLoopTest.RecordingConsumer<HubEventAvro>();
        var snapshots = new KafkaLoopTest.RecordingConsumer<SensorsSnapshotAvro>();
        hubs.closeFailure = new IllegalStateException("Close failed");
        var runner = runner(hubs, snapshots, mock(HubEventService.class));
        assertThrows(IllegalStateException.class, runner::close);
        assertEquals(1, snapshots.closes);
    }

    private AnalysisRunner runner(KafkaLoopTest.RecordingConsumer<HubEventAvro> hubs,
                                  KafkaLoopTest.RecordingConsumer<SensorsSnapshotAvro> snapshots,
                                  HubEventService service) {
        return new AnalysisRunner(new HubEventProcessor(hubs, KafkaLoopTest.SETTINGS, service),
                new SnapshotProcessor(snapshots, KafkaLoopTest.SETTINGS,
                        mock(SnapshotAnalysisService.class), mock(HubRouterClient.class)));
    }
}
