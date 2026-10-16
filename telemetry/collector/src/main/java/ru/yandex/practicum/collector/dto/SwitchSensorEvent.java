package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class SwitchSensorEvent extends SensorEvent {
    @NotNull public Boolean state;

    @Override public String getType() { return "SWITCH_SENSOR_EVENT"; }
}
