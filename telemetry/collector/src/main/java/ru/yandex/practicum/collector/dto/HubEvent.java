package ru.yandex.practicum.collector.dto;
import com.fasterxml.jackson.annotation.*;
import jakarta.validation.constraints.*;
import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import ru.yandex.practicum.kafka.telemetry.serialization.json.StrictInstantDeserializer;
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = DeviceAddedEvent.class, name = "DEVICE_ADDED"),
    @JsonSubTypes.Type(value = DeviceRemovedEvent.class, name = "DEVICE_REMOVED"),
    @JsonSubTypes.Type(value = ScenarioAddedEvent.class, name = "SCENARIO_ADDED"),
    @JsonSubTypes.Type(value = ScenarioRemovedEvent.class, name = "SCENARIO_REMOVED")
})
public abstract class HubEvent {
    @NotBlank public String hubId;
    @NotNull @JsonDeserialize(using = StrictInstantDeserializer.class)
    public Instant timestamp = Instant.now();

    public abstract String getType();
}
