package ru.yandex.practicum.collector.config;

import java.util.HashMap;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaSettings.class)
public class KafkaConfiguration {

    @Bean(destroyMethod = "")
    Producer<String, SpecificRecord> producer(KafkaSettings settings) {
        var properties = new HashMap<>(settings.properties());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        return new KafkaProducer<>(properties);
    }
}
