package ru.yandex.practicum.analyzer.config;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("analyzer.kafka")
public record KafkaSettings(String bootstrapServers, String hubsTopic, String snapshotsTopic,
                            String hubsGroupId, String snapshotsGroupId,
                            Map<String, Object> hubProperties, Map<String, Object> snapshotProperties,
                            Duration pollTimeout, Duration closeTimeout) {
}
