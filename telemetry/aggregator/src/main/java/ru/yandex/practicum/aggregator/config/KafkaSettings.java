package ru.yandex.practicum.aggregator.config;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("aggregator.kafka")
public record KafkaSettings(String bootstrapServers, String sensorsTopic, String snapshotsTopic,
                            String groupId, Map<String, Object> consumerProperties,
                            Map<String, Object> producerProperties, Duration pollTimeout,
                            Duration closeTimeout) { }
