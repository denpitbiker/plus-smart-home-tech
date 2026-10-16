package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class ClimateSensorEvent extends SensorEvent {
    @NotNull public Integer temperatureC;
    @NotNull public Integer humidity;
    @NotNull public Integer co2Level;

    @Override public String getType() { return "CLIMATE_SENSOR_EVENT"; }
}
