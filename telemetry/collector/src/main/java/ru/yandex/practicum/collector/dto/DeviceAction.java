package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class DeviceAction {
    @NotBlank public String sensorId;
    @NotNull public ActionType type;
    public Integer value;
}
