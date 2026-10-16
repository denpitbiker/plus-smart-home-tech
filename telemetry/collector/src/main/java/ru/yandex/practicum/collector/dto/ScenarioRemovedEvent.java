package ru.yandex.practicum.collector.dto;
import jakarta.validation.constraints.*;
public final class ScenarioRemovedEvent extends HubEvent {
    @NotBlank @Size(min = 3) public String name;

    @Override public String getType() { return "SCENARIO_REMOVED"; }
}
