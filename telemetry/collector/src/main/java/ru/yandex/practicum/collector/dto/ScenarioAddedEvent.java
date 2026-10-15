package ru.yandex.practicum.collector.dto;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
public final class ScenarioAddedEvent extends HubEvent {
    @NotBlank @Size(min = 3) public String name;
    @NotEmpty public java.util.List<@NotNull @Valid ScenarioCondition> conditions;
    @NotEmpty public java.util.List<@NotNull @Valid DeviceAction> actions;

    @Override public String getType() { return "SCENARIO_ADDED"; }
}
