package ru.yandex.practicum.collector.dto;
import com.fasterxml.jackson.annotation.*;
import jakarta.validation.constraints.*;
import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import ru.yandex.practicum.kafka.telemetry.serialization.json.StrictInstantDeserializer;
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ClimateSensorEvent.class, name = "CLIMATE_SENSOR_EVENT"),
    @JsonSubTypes.Type(value = LightSensorEvent.class, name = "LIGHT_SENSOR_EVENT"),
    @JsonSubTypes.Type(value = MotionSensorEvent.class, name = "MOTION_SENSOR_EVENT"),
    @JsonSubTypes.Type(value = SwitchSensorEvent.class, name = "SWITCH_SENSOR_EVENT"),
    @JsonSubTypes.Type(value = TemperatureSensorEvent.class, name = "TEMPERATURE_SENSOR_EVENT")
})
public abstract class SensorEvent {
    @NotBlank public String id;
    @NotBlank public String hubId;
    @NotNull @JsonDeserialize(using = StrictInstantDeserializer.class)
    public Instant timestamp = Instant.now();

    public abstract String getType();
}
