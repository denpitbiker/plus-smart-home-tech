package ru.yandex.practicum.collector.service;

import java.util.concurrent.CompletableFuture;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

public interface EventCollector {
    CompletableFuture<Void> collect(SensorEventAvro event);

    CompletableFuture<Void> collect(HubEventAvro event);
}
