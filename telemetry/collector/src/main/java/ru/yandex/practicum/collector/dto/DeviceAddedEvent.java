package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class DeviceAddedEvent extends HubEvent {
    @NotBlank public String id;
    @NotNull public DeviceType deviceType;

    @Override public String getType() { return "DEVICE_ADDED"; }
}
