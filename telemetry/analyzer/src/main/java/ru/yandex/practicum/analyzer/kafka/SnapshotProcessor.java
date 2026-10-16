package ru.yandex.practicum.analyzer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.analysis.SnapshotAnalysisService;
import ru.yandex.practicum.analyzer.config.KafkaSettings;
import ru.yandex.practicum.analyzer.grpc.HubRouterClient;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Component
public class SnapshotProcessor {
    final KafkaLoop<SensorsSnapshotAvro> loop;

    public SnapshotProcessor(Consumer<String, SensorsSnapshotAvro> consumer, KafkaSettings settings,
                             SnapshotAnalysisService analysis, HubRouterClient client) {
        loop = new KafkaLoop<>(consumer, settings, settings.snapshotsTopic(),
                snapshot -> client.send(analysis.analyze(snapshot)), SensorsSnapshotAvro::getHubId);
    }

    public void start() {
        loop.run();
    }
}
