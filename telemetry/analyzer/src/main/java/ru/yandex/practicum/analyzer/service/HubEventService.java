package ru.yandex.practicum.analyzer.service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.Action;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.analyzer.model.Condition;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.ScenarioAction;
import ru.yandex.practicum.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.analyzer.model.Sensor;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

@Service
@Transactional
public class HubEventService {
    private final SensorRepository sensors;
    private final ScenarioRepository scenarios;
    private final ConditionRepository conditions;
    private final ActionRepository actions;
    private final ScenarioConditionRepository scenarioConditions;
    private final ScenarioActionRepository scenarioActions;

    public HubEventService(SensorRepository sensors, ScenarioRepository scenarios,
                           ConditionRepository conditions, ActionRepository actions,
                           ScenarioConditionRepository scenarioConditions,
                           ScenarioActionRepository scenarioActions) {
        this.sensors = sensors;
        this.scenarios = scenarios;
        this.conditions = conditions;
        this.actions = actions;
        this.scenarioConditions = scenarioConditions;
        this.scenarioActions = scenarioActions;
    }

    public void handle(HubEventAvro event) {
        require(event != null, "Hub event is required");
        requireText(event.getHubId(), "Hub ID");
        require(event.getTimestamp() != null, "Event timestamp is required");
        String hubId = event.getHubId();
        Object payload = event.getPayload();
        if (payload instanceof DeviceAddedEventAvro added) {
            addSensor(hubId, added);
        } else if (payload instanceof DeviceRemovedEventAvro removed) {
            removeSensor(hubId, removed.getId());
        } else if (payload instanceof ScenarioAddedEventAvro added) {
            saveScenario(hubId, added);
        } else if (payload instanceof ScenarioRemovedEventAvro removed) {
            requireName(removed.getName());
            scenarios.findByHubIdAndName(hubId, removed.getName()).ifPresent(this::removeScenario);
        } else {
            throw new IllegalArgumentException("Unsupported or missing hub event payload");
        }
    }

    private void addSensor(String hubId, DeviceAddedEventAvro event) {
        requireText(event.getId(), "Sensor ID");
        require(event.getType() != null, "Device type is required");
        var existing = sensors.findById(event.getId());
        if (existing.isPresent()) {
            require(existing.get().getHubId().equals(hubId), "Sensor belongs to another hub");
            return;
        }
        sensors.save(new Sensor(event.getId(), hubId));
    }

    private void removeSensor(String hubId, String sensorId) {
        requireText(sensorId, "Sensor ID");
        sensors.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
            Set<Long> affected = new HashSet<>(scenarioConditions.findScenarioIdsBySensorId(sensorId));
            affected.addAll(scenarioActions.findScenarioIdsBySensorId(sensorId));
            for (Long scenarioId : affected) {
                removeScenario(scenarios.findById(scenarioId).orElseThrow());
            }
            sensors.delete(sensor);
        });
    }

    private void saveScenario(String hubId, ScenarioAddedEventAvro event) {
        requireName(event.getName());
        require(event.getConditions() != null && !event.getConditions().isEmpty(), "Conditions are required");
        require(event.getActions() != null && !event.getActions().isEmpty(), "Actions are required");
        Set<String> ids = new HashSet<>();
        for (var condition : event.getConditions()) {
            require(condition != null, "Condition is required");
            requireText(condition.getSensorId(), "Condition sensor ID");
            require(condition.getType() != null && condition.getOperation() != null,
                    "Condition type and operation are required");
            conditionValue(condition);
            ids.add(condition.getSensorId());
        }
        for (var action : event.getActions()) {
            require(action != null, "Action is required");
            requireText(action.getSensorId(), "Action sensor ID");
            require(action.getType() != null, "Action type is required");
            ids.add(action.getSensorId());
        }
        Map<String, Sensor> registered = sensors.findAllById(ids).stream()
                .collect(Collectors.toMap(Sensor::getId, Function.identity()));
        require(registered.size() == ids.size()
                        && registered.values().stream().allMatch(sensor -> sensor.getHubId().equals(hubId)),
                "All scenario devices must be registered in the same hub");

        Scenario scenario = scenarios.findByHubIdAndName(hubId, event.getName())
                .orElseGet(() -> scenarios.saveAndFlush(new Scenario(hubId, event.getName())));
        removeScenarioContents(scenario);
        for (var condition : event.getConditions()) {
            Condition saved = conditions.saveAndFlush(new Condition(
                    ConditionType.valueOf(condition.getType().name()),
                    ConditionOperation.valueOf(condition.getOperation().name()), conditionValue(condition)));
            scenarioConditions.save(new ScenarioCondition(scenario, registered.get(condition.getSensorId()), saved));
        }
        for (var action : event.getActions()) {
            Action saved = actions.saveAndFlush(new Action(ActionType.valueOf(action.getType().name()), action.getValue()));
            scenarioActions.save(new ScenarioAction(scenario, registered.get(action.getSensorId()), saved));
        }
    }

    private void removeScenario(Scenario scenario) {
        removeScenarioContents(scenario);
        scenarios.delete(scenario);
        scenarios.flush();
    }

    private void removeScenarioContents(Scenario scenario) {
        var oldConditions = scenarioConditions.findByScenarioIdOrderByConditionId(scenario.getId());
        var oldActions = scenarioActions.findByScenarioIdOrderByActionId(scenario.getId());
        var conditionIds = oldConditions.stream().map(link -> link.getCondition().getId()).toList();
        var actionIds = oldActions.stream().map(link -> link.getAction().getId()).toList();
        scenarioConditions.deleteAll(oldConditions);
        scenarioActions.deleteAll(oldActions);
        scenarioConditions.flush();
        scenarioActions.flush();
        conditions.deleteAllByIdInBatch(conditionIds);
        actions.deleteAllByIdInBatch(actionIds);
    }

    private static Integer conditionValue(ScenarioConditionAvro condition) {
        Object value = condition.getValue();
        if (value == null) {
            return null;
        }
        boolean logical = condition.getType() == ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.MOTION
                || condition.getType() == ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro.SWITCH;
        require(logical ? value instanceof Boolean : value instanceof Integer,
                "Condition value does not match its type");
        return value instanceof Boolean bool ? (bool ? 1 : 0) : (Integer) value;
    }

    private static void requireName(String name) {
        requireText(name, "Scenario name");
        require(name.length() >= 3, "Scenario name must contain at least three characters");
    }

    private static void requireText(String text, String field) {
        require(text != null && !text.isBlank(), field + " must not be blank");
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
