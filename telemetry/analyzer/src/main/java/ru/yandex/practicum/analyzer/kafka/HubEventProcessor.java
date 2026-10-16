package ru.yandex.practicum.analyzer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.config.KafkaSettings;
import ru.yandex.practicum.analyzer.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Component
public class HubEventProcessor implements Runnable {
    final KafkaLoop<HubEventAvro> loop;

    public HubEventProcessor(Consumer<String, HubEventAvro> consumer, KafkaSettings settings, HubEventService service) {
        loop = new KafkaLoop<>(consumer, settings, settings.hubsTopic(), service::handle, HubEventAvro::getHubId);
    }

    @Override
    public void run() {
        loop.run();
    }
}
