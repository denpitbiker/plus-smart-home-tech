package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class LightSensorEvent extends SensorEvent {
    @NotNull public Integer linkQuality = 0;
    @NotNull public Integer luminosity = 0;

    @Override public String getType() { return "LIGHT_SENSOR_EVENT"; }
}
