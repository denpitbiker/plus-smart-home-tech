package ru.yandex.practicum.collector.config;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector.kafka")
public record KafkaSettings(String bootstrapServers, String sensorsTopic, String hubsTopic,
                            Map<String, Object> properties, Duration closeTimeout) { }
