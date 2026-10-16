package ru.yandex.practicum.collector.http;
import java.util.concurrent.CompletableFuture;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.dto.*;
import ru.yandex.practicum.collector.service.EventCollector;
import ru.yandex.practicum.collector.mapping.SensorEventMapper;
import ru.yandex.practicum.collector.mapping.HubEventMapper;
@RestController
@RequestMapping("/events")
public class EventController {
    private final EventCollector collector;

    private final SensorEventMapper sensors;
    private final HubEventMapper hubs;

    public EventController(EventCollector collector, SensorEventMapper sensors, HubEventMapper hubs) {
        this.collector = collector;
        this.sensors = sensors;
        this.hubs = hubs;
    }

    @PostMapping(value = "/sensors", consumes = "application/json")
    public CompletableFuture<ResponseEntity<Void>> sensors(@Valid @RequestBody SensorEvent event) {
        return collector.collect(sensors.map(event)).thenApply(ignored -> ResponseEntity.ok().build());
    }

    @PostMapping(value = "/hubs", consumes = "application/json")
    public CompletableFuture<ResponseEntity<Void>> hubs(@Valid @RequestBody HubEvent event) {
        return collector.collect(hubs.map(event)).thenApply(ignored -> ResponseEntity.ok().build());
    }
}
