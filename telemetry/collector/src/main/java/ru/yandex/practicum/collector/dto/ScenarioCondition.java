package ru.yandex.practicum.collector.dto;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.*;
public final class ScenarioCondition {
    @NotBlank public String sensorId;
    @NotNull public ConditionType type;
    @NotNull public ConditionOperation operation;
    public Integer value;

    @JsonIgnore @AssertTrue(message = "Logical conditions accept only 0 or 1")
    public boolean isLogicalValueValid() {
        return value == null || (type != ConditionType.MOTION && type != ConditionType.SWITCH)
                || value == 0 || value == 1;
    }
}
