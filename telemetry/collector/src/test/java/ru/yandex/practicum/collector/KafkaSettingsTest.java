package ru.yandex.practicum.collector;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.collector.config.KafkaSettings;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaSettingsTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(BindingConfiguration.class);

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KafkaSettings.class)
    static class BindingConfiguration { }

    @Test
    void bindsProducerPropertiesFromApplicationYaml() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            var settings = context.getBean(KafkaSettings.class);
            assertThat(settings.closeTimeout()).isEqualTo(Duration.ofSeconds(10));
            assertThat(settings.properties()).containsKeys("acks", "enable.idempotence", "linger.ms",
                    "delivery.timeout.ms", "request.timeout.ms", "max.block.ms", "key.serializer", "value.serializer");
            assertThat(settings.properties().get("acks")).isEqualTo("all");
            assertThat(settings.properties().get("linger.ms").toString()).isEqualTo("5");
            assertThat(settings.properties().get("value.serializer"))
                    .isEqualTo("ru.yandex.practicum.kafka.telemetry.serialization.AvroSerializer");
        });
    }

    @Test
    void externalConfigurationOverridesYaml() {
        runner.withPropertyValues("collector.kafka.properties[linger.ms]=27", "collector.kafka.close-timeout=4s",
                "collector.kafka.bootstrap-servers=example:19092").run(context -> {
            assertThat(context).hasNotFailed();
            var settings = context.getBean(KafkaSettings.class);
            assertThat(settings.properties().get("linger.ms").toString()).isEqualTo("27");
            assertThat(settings.closeTimeout()).isEqualTo(Duration.ofSeconds(4));
            assertThat(settings.bootstrapServers()).isEqualTo("example:19092");
        });
    }
}
