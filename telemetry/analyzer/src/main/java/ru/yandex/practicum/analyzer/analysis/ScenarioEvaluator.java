package ru.yandex.practicum.analyzer.analysis;

import java.util.List;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

@Component
public class ScenarioEvaluator {

    public List<DeviceCommand> evaluate(SensorsSnapshotAvro snapshot, List<ScenarioDefinition> scenarios) {
        validate(snapshot);
        return scenarios.stream()
                .filter(scenario -> snapshot.getHubId().equals(scenario.hubId()))
                .filter(scenario -> !scenario.conditions().isEmpty())
                .filter(scenario -> scenario.conditions().stream().allMatch(condition -> matches(snapshot, condition)))
                .flatMap(scenario -> scenario.actions().stream().map(action ->
                        new DeviceCommand(snapshot.getHubId(), scenario.name(), action.sensorId(),
                                action.type(), action.value(), snapshot.getTimestamp())))
                .toList();
    }

    static void validate(SensorsSnapshotAvro snapshot) {
        if (snapshot == null || snapshot.getHubId() == null || snapshot.getHubId().isBlank()
                || snapshot.getTimestamp() == null || snapshot.getSensorsState() == null) {
            throw new IllegalArgumentException("Snapshot hub, timestamp and sensor states are required");
        }
    }

    private boolean matches(SensorsSnapshotAvro snapshot, ScenarioDefinition.Condition condition) {
        if (condition.value() == null || condition.type() == null || condition.operation() == null) {
            return false;
        }
        var state = snapshot.getSensorsState().get(condition.sensorId());
        if (state == null) {
            return false;
        }
        Integer actual = reading(condition.type(), state.getData());
        if (actual == null) {
            return false;
        }
        int comparison = Integer.compare(actual, condition.value());
        return switch (condition.operation()) {
            case EQUALS -> comparison == 0;
            case GREATER_THAN -> comparison > 0;
            case LOWER_THAN -> comparison < 0;
        };
    }

    private Integer reading(ConditionType type, Object payload) {
        return switch (type) {
            case MOTION -> payload instanceof MotionSensorAvro motion ? (motion.getMotion() ? 1 : 0) : null;
            case LUMINOSITY -> payload instanceof LightSensorAvro light ? light.getLuminosity() : null;
            case SWITCH -> payload instanceof SwitchSensorAvro value ? (value.getState() ? 1 : 0) : null;
            case TEMPERATURE -> {
                if (payload instanceof TemperatureSensorAvro temperature) {
                    yield temperature.getTemperatureC();
                }
                yield payload instanceof ClimateSensorAvro climate ? climate.getTemperatureC() : null;
            }
            case CO2LEVEL -> payload instanceof ClimateSensorAvro climate ? climate.getCo2Level() : null;
            case HUMIDITY -> payload instanceof ClimateSensorAvro climate ? climate.getHumidity() : null;
        };
    }
}
