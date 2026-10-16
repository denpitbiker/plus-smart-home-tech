package ru.yandex.practicum.analyzer.config;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaSettingsTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({KafkaSettings.class, HubRouterSettings.class})
    static class Binding { }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner().withUserConfiguration(Binding.class).withInitializer(context -> {
            try {
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"))
                        .forEach(source -> context.getEnvironment().getPropertySources().addLast(source));
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Test
    void bindsDefaultsAndIndependentGroups() {
        runner().run(context -> {
            var settings = context.getBean(KafkaSettings.class);
            assertThat(settings.bootstrapServers()).isEqualTo("localhost:9092");
            assertThat(settings.hubsTopic()).isEqualTo("telemetry.hubs.v1");
            assertThat(settings.snapshotsTopic()).isEqualTo("telemetry.snapshots.v1");
            assertThat(settings.hubsGroupId()).isEqualTo("telemetry-analyzer-hubs");
            assertThat(settings.snapshotsGroupId()).isEqualTo("telemetry-analyzer-snapshots");
            assertThat(settings.hubProperties()).containsEntry("enable.auto.commit", false)
                    .containsEntry("max.poll.records", 100).containsEntry("auto.offset.reset", "earliest");
            assertThat(settings.snapshotProperties()).containsEntry("enable.auto.commit", false)
                    .containsEntry("max.poll.records", 1).containsEntry("max.poll.interval.ms", 300000);
            assertThat(settings.pollTimeout()).isEqualTo(Duration.ofSeconds(1));
            assertThat(settings.closeTimeout()).isEqualTo(Duration.ofSeconds(10));
            assertThat(context.getBean(HubRouterSettings.class).deadline()).isEqualTo(Duration.ofSeconds(5));
            assertThat(context.getEnvironment().getProperty("grpc.client.hub-router.address"))
                    .isEqualTo("static://localhost:59090");
        });
    }

    @Test
    void overridesSettingsAndRejectsAutoCommit() {
        runner().withPropertyValues("analyzer.kafka.bootstrap-servers=example:9093",
                "analyzer.kafka.hubs-group-id=custom-hubs", "analyzer.kafka.snapshots-group-id=custom-snapshots",
                "analyzer.kafka.hubs-topic=custom-events", "analyzer.kafka.snapshots-topic=custom-snapshots",
                "analyzer.kafka.poll-timeout=2s", "analyzer.kafka.close-timeout=3s",
                "analyzer.hub-router.deadline=1s",
                "analyzer.kafka.snapshot-properties[enable.auto.commit]=true").run(context -> {
            var settings = context.getBean(KafkaSettings.class);
            assertThat(settings.bootstrapServers()).isEqualTo("example:9093");
            assertThat(settings.hubsTopic()).isEqualTo("custom-events");
            assertThat(settings.snapshotsTopic()).isEqualTo("custom-snapshots");
            assertThat(settings.hubsGroupId()).isEqualTo("custom-hubs");
            assertThat(settings.snapshotsGroupId()).isEqualTo("custom-snapshots");
            assertThat(settings.pollTimeout()).isEqualTo(Duration.ofSeconds(2));
            assertThat(settings.closeTimeout()).isEqualTo(Duration.ofSeconds(3));
            assertThat(context.getBean(HubRouterSettings.class).deadline()).isEqualTo(Duration.ofSeconds(1));
            assertThrows(IllegalArgumentException.class, () -> new KafkaConfiguration().snapshotConsumer(settings));
        });
    }
}
