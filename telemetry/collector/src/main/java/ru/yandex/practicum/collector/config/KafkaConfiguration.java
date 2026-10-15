package ru.yandex.practicum.collector.config;
import java.util.HashMap;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import ru.yandex.practicum.collector.kafka.AvroSerializer;
@Configuration
@EnableConfigurationProperties(KafkaSettings.class)
public class KafkaConfiguration {
    @Bean(destroyMethod = "close")
    Producer<String, SpecificRecord> producer(KafkaSettings settings) {
        var properties = new HashMap<String, Object>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        return new KafkaProducer<>(properties, new StringSerializer(), new AvroSerializer());
    }
}
