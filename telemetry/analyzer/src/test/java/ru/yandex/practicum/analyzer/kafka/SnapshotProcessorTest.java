package ru.yandex.practicum.analyzer.kafka;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.analyzer.analysis.DeviceCommand;
import ru.yandex.practicum.analyzer.analysis.SnapshotAnalysisService;
import ru.yandex.practicum.analyzer.grpc.HubRouterClient;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SnapshotProcessorTest {

    @Test
    void commitsOnlyAfterAnalysisAndCommandsComplete() {
        verifyProcessing(false);
    }

    @Test
    void failedRpcPreventsCommit() {
        verifyProcessing(true);
    }

    private void verifyProcessing(boolean fail) {
        var consumer = new KafkaLoopTest.RecordingConsumer<SensorsSnapshotAvro>();
        var analysis = mock(SnapshotAnalysisService.class);
        var client = mock(HubRouterClient.class);
        var snapshot = new SensorsSnapshotAvro("hub", Instant.EPOCH, Map.of());
        var commands = List.of(new DeviceCommand("hub", "scene", "sensor", ActionType.ACTIVATE, null, Instant.EPOCH));
        when(analysis.analyze(snapshot)).thenReturn(commands);
        doAnswer(invocation -> {
            assertTrue(consumer.commits.isEmpty());
            if (fail) {
                throw new IllegalStateException("RPC failed");
            }
            return null;
        }).when(client).send(commands);
        var processor = new SnapshotProcessor(consumer, KafkaLoopTest.SETTINGS, analysis, client);
        var partition = new TopicPartition("snapshots", 0);
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(partition));
            consumer.updateBeginningOffsets(Map.of(partition, 0L));
            consumer.addRecord(new ConsumerRecord<>("snapshots", 0, 0, "hub", snapshot));
        });
        consumer.schedulePollTask(processor::requestStop);
        if (fail) {
            assertThrows(IllegalStateException.class, processor::start);
            assertTrue(consumer.commits.isEmpty());
        } else {
            processor.start();
            assertEquals(1L, consumer.commits.getFirst().get(partition).offset());
        }
        var order = inOrder(analysis, client);
        order.verify(analysis).analyze(snapshot);
        order.verify(client).send(commands);
        assertEquals(1, consumer.closes);
    }
}
