package ru.yandex.practicum.collector.service;
import java.util.concurrent.CompletableFuture;
import ru.yandex.practicum.collector.dto.*;
public interface EventCollector {
    CompletableFuture<Void> collect(SensorEvent event);

    CompletableFuture<Void> collect(HubEvent event);
}
