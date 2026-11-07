package ru.yandex.practicum.aggregator.config;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSettingsTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KafkaSettings.class)
    static class Binding { }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner().withUserConfiguration(Binding.class).withInitializer(context -> {
            try {
                var sources = new YamlPropertySourceLoader().load("application", new ClassPathResource("config/application.yaml"));
                sources.forEach(source -> context.getEnvironment().getPropertySources().addLast(source));
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Test
    void bindsKafkaDefaultsFromYaml() {
        runner().run(context -> {
            var settings = context.getBean(KafkaSettings.class);
            assertThat(settings.bootstrapServers()).isEqualTo("localhost:9092");
            assertThat(settings.sensorsTopic()).isEqualTo("telemetry.sensors.v1");
            assertThat(settings.snapshotsTopic()).isEqualTo("telemetry.snapshots.v1");
            assertThat(settings.groupId()).isEqualTo("telemetry-aggregator");
            assertThat(settings.pollTimeout()).isEqualTo(Duration.ofSeconds(1));
            assertThat(settings.closeTimeout()).isEqualTo(Duration.ofSeconds(10));
            assertThat(settings.consumerProperties()).containsEntry("enable.auto.commit", false)
                    .containsEntry("auto.offset.reset", "earliest")
                    .containsEntry("max.poll.records", 100)
                    .containsEntry("value.deserializer", "ru.yandex.practicum.kafka.telemetry.serialization.SensorEventDeserializer");
            assertThat(settings.producerProperties()).containsEntry("acks", "all")
                    .containsEntry("enable.idempotence", true)
                    .containsEntry("linger.ms", 5)
                    .containsEntry("delivery.timeout.ms", 30000)
                    .containsEntry("request.timeout.ms", 10000)
                    .containsEntry("max.block.ms", 5000);
        });
    }

    @Test
    void allowsExternalOverrides() {
        runner().withPropertyValues("aggregator.kafka.bootstrap-servers=example:9093",
                "aggregator.kafka.snapshots-topic=custom", "aggregator.kafka.group-id=custom-group",
                "aggregator.kafka.poll-timeout=2s", "aggregator.kafka.close-timeout=3s",
                "aggregator.kafka.producer-properties[linger.ms]=10").run(context -> {
            var settings = context.getBean(KafkaSettings.class);
            assertThat(settings.bootstrapServers()).isEqualTo("example:9093");
            assertThat(settings.snapshotsTopic()).isEqualTo("custom");
            assertThat(settings.groupId()).isEqualTo("custom-group");
            assertThat(settings.pollTimeout()).isEqualTo(Duration.ofSeconds(2));
            assertThat(settings.closeTimeout()).isEqualTo(Duration.ofSeconds(3));
            assertThat(settings.producerProperties()).containsEntry("linger.ms", "10");
        });
    }
}
