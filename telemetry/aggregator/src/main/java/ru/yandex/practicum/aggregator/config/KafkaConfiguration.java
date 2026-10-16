package ru.yandex.practicum.aggregator.config;

import java.util.HashMap;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Configuration
public class KafkaConfiguration {
    @Bean(destroyMethod = "")
    Consumer<String, SensorEventAvro> consumer(KafkaSettings settings) {
        var properties = new HashMap<>(settings.consumerProperties());
        if (!"false".equalsIgnoreCase(String.valueOf(properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)))) {
            throw new IllegalArgumentException("Aggregator requires enable.auto.commit=false");
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId());
        return new KafkaConsumer<>(properties);
    }

    @Bean(destroyMethod = "")
    Producer<String, SpecificRecord> producer(KafkaSettings settings) {
        var properties = new HashMap<>(settings.producerProperties());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        return new KafkaProducer<>(properties);
    }
}
