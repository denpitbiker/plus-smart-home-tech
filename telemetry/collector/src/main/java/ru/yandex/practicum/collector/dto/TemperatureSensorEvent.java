package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class TemperatureSensorEvent extends SensorEvent {
    @NotNull public Integer temperatureC;
    @NotNull public Integer temperatureF;

    @Override public String getType() { return "TEMPERATURE_SENSOR_EVENT"; }
}
