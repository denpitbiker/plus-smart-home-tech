package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class DeviceRemovedEvent extends HubEvent {
    @NotBlank public String id;

    @Override public String getType() { return "DEVICE_REMOVED"; }
}
