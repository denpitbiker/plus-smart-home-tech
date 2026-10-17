package ru.yandex.practicum.analyzer.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.Sensor;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioWriteRepository;
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
    private final ScenarioWriteRepository writes;

    public HubEventService(SensorRepository sensors, ScenarioRepository scenarios,
                           ScenarioWriteRepository writes) {
        this.sensors = sensors;
        this.scenarios = scenarios;
        this.writes = writes;
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
            var affected = writes.findScenarioIdsBySensorId(sensorId);
            writes.deleteScenarios(affected);
            sensors.deleteById(sensorId);
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

        var newConditions = event.getConditions().stream()
                .map(condition -> new ScenarioWriteRepository.ConditionRow(condition.getSensorId(),
                        ConditionType.valueOf(condition.getType().name()),
                        ConditionOperation.valueOf(condition.getOperation().name()), conditionValue(condition)))
                .toList();
        var newActions = event.getActions().stream()
                .map(action -> new ScenarioWriteRepository.ActionRow(action.getSensorId(),
                        ActionType.valueOf(action.getType().name()), action.getValue()))
                .toList();
        var existing = scenarios.findByHubIdAndName(hubId, event.getName());
        Scenario scenario;
        if (existing.isPresent()) {
            Long scenarioId = existing.get().getId();
            writes.deleteContents(List.of(scenarioId));
            scenario = scenarios.findById(scenarioId).orElseThrow();
        } else {
            scenario = scenarios.save(new Scenario(hubId, event.getName()));
        }
        writes.insertContents(scenario.getId(), newConditions, newActions);
    }

    private void removeScenario(Scenario scenario) {
        writes.deleteScenarios(List.of(scenario.getId()));
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
