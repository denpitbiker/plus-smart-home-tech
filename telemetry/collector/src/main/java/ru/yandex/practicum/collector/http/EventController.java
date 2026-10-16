package ru.yandex.practicum.collector.http;
import java.util.concurrent.CompletableFuture;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.collector.dto.*;
import ru.yandex.practicum.collector.service.EventCollector;
@RestController
@RequestMapping("/events")
public class EventController {
    private final EventCollector collector;

    public EventController(EventCollector collector) { this.collector = collector; }

    @PostMapping(value = "/sensors", consumes = "application/json")
    public CompletableFuture<ResponseEntity<Void>> sensors(@Valid @RequestBody SensorEvent event) {
        return collector.collect(event).thenApply(ignored -> ResponseEntity.ok().build());
    }

    @PostMapping(value = "/hubs", consumes = "application/json")
    public CompletableFuture<ResponseEntity<Void>> hubs(@Valid @RequestBody HubEvent event) {
        return collector.collect(event).thenApply(ignored -> ResponseEntity.ok().build());
    }
}
