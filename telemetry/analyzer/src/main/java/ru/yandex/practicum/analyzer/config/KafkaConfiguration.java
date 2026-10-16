package ru.yandex.practicum.analyzer.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Configuration
public class KafkaConfiguration {

    @Bean(destroyMethod = "")
    public Consumer<String, HubEventAvro> hubConsumer(KafkaSettings settings) {
        return new KafkaConsumer<>(properties(settings, settings.hubsGroupId(), settings.hubProperties()));
    }

    @Bean(destroyMethod = "")
    public Consumer<String, SensorsSnapshotAvro> snapshotConsumer(KafkaSettings settings) {
        return new KafkaConsumer<>(properties(settings, settings.snapshotsGroupId(), settings.snapshotProperties()));
    }

    private Map<String, Object> properties(KafkaSettings settings, String group, Map<String, Object> configured) {
        var result = new HashMap<>(configured);
        if (!"false".equalsIgnoreCase(String.valueOf(result.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)))) {
            throw new IllegalArgumentException("Analyzer requires enable.auto.commit=false");
        }
        if (settings.hubsGroupId().equals(settings.snapshotsGroupId())) {
            throw new IllegalArgumentException("Analyzer consumers require different groups");
        }
        result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        result.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        return result;
    }
}
