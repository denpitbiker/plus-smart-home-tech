package ru.yandex.practicum.analyzer.analysis;

import java.util.List;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;

public record ScenarioDefinition(String hubId, String name,
                                 List<Condition> conditions, List<Action> actions) {

    public ScenarioDefinition {
        conditions = List.copyOf(conditions);
        actions = List.copyOf(actions);
    }

    public record Condition(String sensorId, ConditionType type, ConditionOperation operation, Integer value) {
    }

    public record Action(String sensorId, ActionType type, Integer value) {
    }
}
