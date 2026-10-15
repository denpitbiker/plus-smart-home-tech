package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class MotionSensorEvent extends SensorEvent {
    @NotNull public Integer linkQuality;
    @NotNull public Boolean motion;
    @NotNull public Integer voltage;

    @Override public String getType() { return "MOTION_SENSOR_EVENT"; }
}
