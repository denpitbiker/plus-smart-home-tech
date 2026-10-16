package ru.yandex.practicum.collector;

import java.time.Duration;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.yandex.practicum.collector.config.KafkaSettings;
import ru.yandex.practicum.collector.mapping.HubEventMapper;
import ru.yandex.practicum.collector.mapping.SensorEventMapper;
import ru.yandex.practicum.collector.service.KafkaEventCollector;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class KafkaLifecycleTest {
    @SuppressWarnings("unchecked")
    private Producer<String, SpecificRecord> producer() {
        return mock(Producer.class);
    }

    private KafkaEventCollector collector(Producer<String, SpecificRecord> producer) {
        var settings = new KafkaSettings("localhost:9092", "sensors", "hubs", Map.of(), Duration.ofSeconds(3));
        return new KafkaEventCollector(producer, settings, new SensorEventMapper(), new HubEventMapper());
    }

    @Test
    void springShutdownFlushesThenClosesOnceWithConfiguredTimeout() {
        var producer = producer();
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean("producer", Producer.class, () -> producer, definition -> definition.setDestroyMethodName(""));
            context.registerBean(KafkaEventCollector.class, () -> collector(producer));
            context.refresh();
        }
        var order = inOrder(producer);
        order.verify(producer).flush();
        order.verify(producer).close(Duration.ofSeconds(3));
        verifyNoMoreInteractions(producer);
    }

    @Test
    void stillClosesWhenFlushThrows() {
        var producer = producer();
        var failure = new IllegalStateException("flush failed");
        doThrow(failure).when(producer).flush();
        assertThatThrownBy(() -> collector(producer).close()).isSameAs(failure);
        var order = inOrder(producer);
        order.verify(producer).flush();
        order.verify(producer).close(Duration.ofSeconds(3));
        verifyNoMoreInteractions(producer);
    }
}
