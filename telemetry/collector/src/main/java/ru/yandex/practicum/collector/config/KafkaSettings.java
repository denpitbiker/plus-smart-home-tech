package ru.yandex.practicum.collector.config;
import org.springframework.boot.context.properties.ConfigurationProperties;
@ConfigurationProperties("collector.kafka")
public record KafkaSettings(String bootstrapServers, String sensorsTopic, String hubsTopic) { }
