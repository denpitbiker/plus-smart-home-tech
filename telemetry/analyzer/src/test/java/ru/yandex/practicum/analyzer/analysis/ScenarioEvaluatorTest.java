package ru.yandex.practicum.analyzer.analysis;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import static org.junit.jupiter.api.Assertions.*;

class ScenarioEvaluatorTest {
    private static final Instant TIME = Instant.parse("2024-08-06T15:11:24.157Z");
    private final ScenarioEvaluator evaluator = new ScenarioEvaluator();

    static Stream<Arguments> comparisons() {
        List<Object[]> readings = List.of(
                new Object[]{ConditionType.MOTION, new MotionSensorAvro(20, true, 220), 1},
                new Object[]{ConditionType.MOTION, new MotionSensorAvro(20, false, 220), 0},
                new Object[]{ConditionType.SWITCH, new SwitchSensorAvro(false), 0},
                new Object[]{ConditionType.SWITCH, new SwitchSensorAvro(true), 1},
                new Object[]{ConditionType.LUMINOSITY, new LightSensorAvro(100, 20), 20},
                new Object[]{ConditionType.TEMPERATURE, new TemperatureSensorAvro("sensor", "hub", TIME, -10, 14), -10},
                new Object[]{ConditionType.TEMPERATURE, new ClimateSensorAvro(25, 60, 500), 25},
                new Object[]{ConditionType.HUMIDITY, new ClimateSensorAvro(25, 60, 500), 60},
                new Object[]{ConditionType.CO2LEVEL, new ClimateSensorAvro(25, 60, 500), 500});
        return readings.stream().flatMap(reading ->
                Stream.of(ConditionOperation.values()).flatMap(operation ->
                        Stream.of(-1, 0, 1).map(delta -> {
                            boolean expected = switch (operation) {
                                case EQUALS -> delta == 0;
                                case GREATER_THAN -> delta < 0;
                                case LOWER_THAN -> delta > 0;
                            };
                            return Arguments.of(reading[0], reading[1], operation, (Integer) reading[2] + delta, expected);
                        })));
    }

    @ParameterizedTest
    @MethodSource("comparisons")
    void comparesAllReadingsAndOperations(ConditionType type, Object payload,
                                         ConditionOperation operation, int reference, boolean expected) {
        var scenario = scenario(List.of(new ScenarioDefinition.Condition("sensor", type, operation, reference)));
        assertEquals(expected ? 1 : 0, evaluator.evaluate(snapshot(payload), List.of(scenario)).size());
    }

    @Test
    void comparesIntegerExtremesWithoutOverflow() {
        var condition = new ScenarioDefinition.Condition("sensor", ConditionType.LUMINOSITY,
                ConditionOperation.GREATER_THAN, Integer.MIN_VALUE);
        assertEquals(1, evaluator.evaluate(snapshot(new LightSensorAvro(0, Integer.MAX_VALUE)),
                List.of(scenario(List.of(condition)))).size());
    }

    @Test
    void requiresAllConditionsAndDoesNotTriggerEmptyOrMissingReadings() {
        var motion = new ScenarioDefinition.Condition("motion", ConditionType.MOTION, ConditionOperation.EQUALS, 1);
        var light = new ScenarioDefinition.Condition("light", ConditionType.LUMINOSITY, ConditionOperation.LOWER_THAN, 20);
        var scene = scenario(List.of(motion, light));
        var snapshot = new SensorsSnapshotAvro("hub", TIME, Map.of(
                "motion", new SensorStateAvro(TIME, new MotionSensorAvro(0, true, 0)),
                "light", new SensorStateAvro(TIME, new LightSensorAvro(0, 19))));
        assertEquals(1, evaluator.evaluate(snapshot, List.of(scene)).size());
        snapshot.setSensorsState(Map.of("motion", new SensorStateAvro(TIME, new MotionSensorAvro(0, true, 0))));
        assertTrue(evaluator.evaluate(snapshot, List.of(scene)).isEmpty());
        snapshot.setSensorsState(Map.of(
                "motion", new SensorStateAvro(TIME, new MotionSensorAvro(0, false, 0)),
                "light", new SensorStateAvro(TIME, new LightSensorAvro(0, 19))));
        assertTrue(evaluator.evaluate(snapshot, List.of(scene)).isEmpty());
        assertTrue(evaluator.evaluate(snapshot, List.of(scenario(List.of()))).isEmpty());
    }

    @Test
    void rejectsWrongPayloadAndNullReferenceWithoutTriggering() {
        for (ConditionType type : ConditionType.values()) {
            var condition = new ScenarioDefinition.Condition("sensor", type, ConditionOperation.EQUALS, null);
            assertTrue(evaluator.evaluate(snapshot(new SwitchSensorAvro(false)),
                    List.of(scenario(List.of(condition)))).isEmpty());
        }
        var condition = new ScenarioDefinition.Condition("sensor", ConditionType.MOTION, ConditionOperation.EQUALS, 0);
        assertTrue(evaluator.evaluate(snapshot(new SwitchSensorAvro(false)), List.of(scenario(List.of(condition)))).isEmpty());
        var state = new SensorStateAvro();
        var missingPayload = new SensorsSnapshotAvro("hub", TIME, Map.of("sensor", state));
        assertTrue(evaluator.evaluate(missingPayload, List.of(scenario(List.of(condition)))).isEmpty());
    }

    @Test
    void preservesCommandOrderMetadataAndNullableValuesOnEverySnapshot() {
        var conditions = List.of(new ScenarioDefinition.Condition("sensor", ConditionType.SWITCH,
                ConditionOperation.EQUALS, 0));
        var actions = Stream.of(ActionType.values())
                .map(type -> new ScenarioDefinition.Action("target-not-in-snapshot", type,
                        type == ActionType.SET_VALUE ? 0 : null)).toList();
        var first = new ScenarioDefinition("hub", "first", conditions, actions);
        var second = new ScenarioDefinition("hub", "second", conditions, actions);
        var other = new ScenarioDefinition("other", "foreign", conditions, actions);
        var snapshot = snapshot(new SwitchSensorAvro(false));
        var result = evaluator.evaluate(snapshot, List.of(first, other, second));
        assertEquals(8, result.size());
        for (int i = 0; i < result.size(); i++) {
            var action = actions.get(i % actions.size());
            assertEquals(new DeviceCommand("hub", i < 4 ? "first" : "second",
                    action.sensorId(), action.type(), action.value(), TIME), result.get(i));
        }
        assertEquals(result, evaluator.evaluate(snapshot, List.of(first, other, second)));
        snapshot.setTimestamp(TIME.minusSeconds(10));
        assertEquals(TIME.minusSeconds(10),
                evaluator.evaluate(snapshot, List.of(first)).getFirst().timestamp());
    }

    @Test
    void definitionsAndCommandsAreImmutable() {
        var conditions = new ArrayList<>(List.of(new ScenarioDefinition.Condition("sensor", ConditionType.SWITCH,
                ConditionOperation.EQUALS, 0)));
        var actions = new ArrayList<>(List.of(new ScenarioDefinition.Action("target", ActionType.ACTIVATE, null)));
        var scene = new ScenarioDefinition("hub", "scenario", conditions, actions);
        conditions.clear();
        actions.clear();
        var result = evaluator.evaluate(snapshot(new SwitchSensorAvro(false)), List.of(scene));
        assertEquals(1, result.size());
        assertThrows(UnsupportedOperationException.class, () -> scene.conditions().clear());
        assertThrows(UnsupportedOperationException.class, result::clear);
    }

    @Test
    void rejectsMissingSnapshotMetadata() {
        assertThrows(IllegalArgumentException.class, () -> evaluator.evaluate(null, List.of()));
        assertThrows(IllegalArgumentException.class, () -> evaluator.evaluate(new SensorsSnapshotAvro(), List.of()));
    }

    private ScenarioDefinition scenario(List<ScenarioDefinition.Condition> conditions) {
        return new ScenarioDefinition("hub", "scenario", conditions,
                List.of(new ScenarioDefinition.Action("target", ActionType.ACTIVATE, null)));
    }

    private SensorsSnapshotAvro snapshot(Object payload) {
        return new SensorsSnapshotAvro("hub", TIME, Map.of("sensor", new SensorStateAvro(TIME, payload)));
    }
}
