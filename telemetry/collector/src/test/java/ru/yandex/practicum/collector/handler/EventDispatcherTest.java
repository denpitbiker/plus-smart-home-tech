package ru.yandex.practicum.collector.handler;

import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.serialization.AvroSerializer;

import static org.junit.jupiter.api.Assertions.*;

class EventDispatcherTest {
    private static final Timestamp TS = Timestamp.newBuilder().setSeconds(1722957084).setNanos(157999999).build();
    private static final Instant TIME = Instant.ofEpochSecond(TS.getSeconds(), 157000000);
    private final EventDispatcher dispatcher = new EventDispatcher(sensors(), hubs());

    private static List<SensorEventHandler> sensors() {
        return List.of(new MotionSensorEventHandler(), new TemperatureSensorEventHandler(),
                new LightSensorEventHandler(), new ClimateSensorEventHandler(), new SwitchSensorEventHandler());
    }

    private static List<HubEventHandler> hubs() {
        return List.of(new DeviceAddedEventHandler(), new DeviceRemovedEventHandler(),
                new ScenarioAddedEventHandler(), new ScenarioRemovedEventHandler());
    }

    private static SensorEventProto.Builder sensor() {
        return SensorEventProto.newBuilder().setId("sensor-1").setHubId("hub-1").setTimestamp(TS);
    }

    private static HubEventProto.Builder hub() {
        return HubEventProto.newBuilder().setHubId("hub-1").setTimestamp(TS);
    }

    private static ScenarioAddedEventProto.Builder scenario() {
        return ScenarioAddedEventProto.newBuilder().setName("night")
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("motion"))
                .addAction(DeviceActionProto.newBuilder().setSensorId("lamp"));
    }

    static Stream<Object[]> sensorCases() {
        return Stream.of(
                new Object[]{sensor().setMotionSensor(MotionSensorProto.newBuilder().setLinkQuality(75).setMotion(true).setVoltage(220)).build(), new MotionSensorAvro(75, true, 220)},
                new Object[]{sensor().setTemperatureSensor(TemperatureSensorProto.newBuilder().setTemperatureC(-5).setTemperatureF(23)).build(), new TemperatureSensorAvro("sensor-1", "hub-1", TIME, -5, 23)},
                new Object[]{sensor().setLightSensor(LightSensorProto.newBuilder().setLinkQuality(90).setLuminosity(45)).build(), new LightSensorAvro(90, 45)},
                new Object[]{sensor().setClimateSensor(ClimateSensorProto.newBuilder().setTemperatureC(22).setHumidity(60).setCo2Level(500)).build(), new ClimateSensorAvro(22, 60, 500)},
                new Object[]{sensor().setSwitchSensor(SwitchSensorProto.getDefaultInstance()).build(), new SwitchSensorAvro(false)});
    }

    @ParameterizedTest
    @MethodSource("sensorCases")
    void mapsSensorsAndRoundTrips(SensorEventProto input, SpecificRecord payload) throws Exception {
        var result = dispatcher.map(input);
        assertEquals(new SensorEventAvro("sensor-1", "hub-1", TIME, payload), result);
        roundTrip(result);
    }

    static Stream<HubEventProto> hubCases() {
        return Stream.of(
                hub().setDeviceAdded(DeviceAddedEventProto.newBuilder().setId("device").setType(DeviceTypeProto.LIGHT_SENSOR)).build(),
                hub().setDeviceRemoved(DeviceRemovedEventProto.newBuilder().setId("device")).build(),
                hub().setScenarioAdded(scenario()).build(),
                hub().setScenarioRemoved(ScenarioRemovedEventProto.newBuilder().setName("night")).build());
    }

    @ParameterizedTest
    @MethodSource("hubCases")
    void mapsHubsAndRoundTrips(HubEventProto input) throws Exception {
        var result = dispatcher.map(input);
        Object expected = switch (input.getPayloadCase()) {
            case DEVICE_ADDED -> new DeviceAddedEventAvro("device", DeviceTypeAvro.LIGHT_SENSOR);
            case DEVICE_REMOVED -> new DeviceRemovedEventAvro("device");
            case SCENARIO_REMOVED -> new ScenarioRemovedEventAvro("night");
            case SCENARIO_ADDED -> new ScenarioAddedEventAvro("night",
                    List.of(new ScenarioConditionAvro("motion", ConditionTypeAvro.MOTION, ConditionOperationAvro.EQUALS, null)),
                    List.of(new DeviceActionAvro("lamp", ActionTypeAvro.ACTIVATE, null)));
            default -> throw new AssertionError();
        };
        assertEquals(new HubEventAvro("hub-1", TIME, expected), result);
        roundTrip(result);
    }

    private static void roundTrip(SpecificRecord record) throws Exception {
        byte[] bytes = new AvroSerializer().serialize("topic", record);
        Object decoded = new SpecificDatumReader<>(record.getSchema())
                .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        assertEquals(record, decoded);
    }

    @Test
    void preservesScenarioOrderAndValuePresence() throws Exception {
        var p = scenario().clearCondition().clearAction()
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("switch").setType(ConditionTypeProto.SWITCH).setBoolValue(false))
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("motion").setBoolValue(true))
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("temp").setType(ConditionTypeProto.TEMPERATURE).setIntValue(0))
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("other"))
                .addAction(DeviceActionProto.newBuilder().setSensorId("one").setValue(0))
                .addAction(DeviceActionProto.newBuilder().setSensorId("two"));
        var record = dispatcher.map(hub().setScenarioAdded(p).build());
        var result = (ScenarioAddedEventAvro) record.getPayload();
        assertEquals(List.of("switch", "motion", "temp", "other"), result.getConditions().stream().map(c -> c.getSensorId().toString()).toList());
        assertEquals(false, result.getConditions().get(0).getValue());
        assertEquals(true, result.getConditions().get(1).getValue());
        assertEquals(0, result.getConditions().get(2).getValue());
        assertNull(result.getConditions().get(3).getValue());
        assertEquals("one", result.getActions().get(0).getSensorId().toString());
        assertEquals(0, result.getActions().get(0).getValue());
        assertEquals("two", result.getActions().get(1).getSensorId().toString());
        assertNull(result.getActions().get(1).getValue());
        roundTrip(record);
    }

    @ParameterizedTest
    @EnumSource(value = DeviceTypeProto.class, names = "UNRECOGNIZED", mode = EnumSource.Mode.EXCLUDE)
    void mapsDeviceEnums(DeviceTypeProto value) {
        var p = (DeviceAddedEventAvro) dispatcher.map(hub().setDeviceAdded(
                DeviceAddedEventProto.newBuilder().setId("device").setType(value)).build()).getPayload();
        assertEquals(value.name(), p.getType().name());
    }

    @ParameterizedTest
    @EnumSource(value = ConditionTypeProto.class, names = "UNRECOGNIZED", mode = EnumSource.Mode.EXCLUDE)
    void mapsConditionEnums(ConditionTypeProto value) {
        var c = ScenarioConditionProto.newBuilder().setSensorId("sensor").setType(value);
        if (value == ConditionTypeProto.MOTION || value == ConditionTypeProto.SWITCH) {
            c.setBoolValue(false);
        } else {
            c.setIntValue(-1);
        }
        var p = (ScenarioAddedEventAvro) dispatcher.map(hub().setScenarioAdded(scenario().setCondition(0, c)).build()).getPayload();
        assertEquals(value.name(), p.getConditions().getFirst().getType().name());
    }

    @ParameterizedTest
    @EnumSource(value = ConditionOperationProto.class, names = "UNRECOGNIZED", mode = EnumSource.Mode.EXCLUDE)
    void mapsOperationEnums(ConditionOperationProto value) {
        var p = (ScenarioAddedEventAvro) dispatcher.map(hub().setScenarioAdded(scenario().setCondition(0,
                ScenarioConditionProto.newBuilder().setSensorId("sensor").setOperation(value))).build()).getPayload();
        assertEquals(value.name(), p.getConditions().getFirst().getOperation().name());
    }

    @ParameterizedTest
    @EnumSource(value = ActionTypeProto.class, names = "UNRECOGNIZED", mode = EnumSource.Mode.EXCLUDE)
    void mapsActionEnums(ActionTypeProto value) {
        var p = (ScenarioAddedEventAvro) dispatcher.map(hub().setScenarioAdded(scenario().setAction(0,
                DeviceActionProto.newBuilder().setSensorId("sensor").setType(value))).build()).getPayload();
        assertEquals(value.name(), p.getActions().getFirst().getType().name());
    }

    static Stream<HubEventProto> invalidHubs() {
        var valid = hub().setScenarioAdded(scenario()).build();
        return Stream.of(hub().build(), valid.toBuilder().setHubId(" ").build(),
                hub().setDeviceAdded(DeviceAddedEventProto.newBuilder().setId("device").setTypeValue(99)).build(),
                hub().setDeviceAdded(DeviceAddedEventProto.getDefaultInstance()).build(),
                hub().setDeviceRemoved(DeviceRemovedEventProto.getDefaultInstance()).build(),
                hub().setScenarioRemoved(ScenarioRemovedEventProto.newBuilder().setName("ab")).build(),
                hub().setScenarioAdded(scenario().setName("   ")).build(),
                hub().setScenarioAdded(scenario().clearCondition()).build(),
                hub().setScenarioAdded(scenario().clearAction()).build(),
                hub().setScenarioAdded(scenario().setCondition(0, ScenarioConditionProto.newBuilder().setSensorId("x").setTypeValue(99))).build(),
                hub().setScenarioAdded(scenario().setCondition(0, ScenarioConditionProto.newBuilder().setSensorId("x").setOperationValue(99))).build(),
                hub().setScenarioAdded(scenario().setCondition(0, ScenarioConditionProto.getDefaultInstance())).build(),
                hub().setScenarioAdded(scenario().setCondition(0, ScenarioConditionProto.newBuilder().setSensorId("x").setIntValue(1))).build(),
                hub().setScenarioAdded(scenario().setCondition(0, ScenarioConditionProto.newBuilder().setSensorId("x").setType(ConditionTypeProto.HUMIDITY).setBoolValue(true))).build(),
                hub().setScenarioAdded(scenario().setAction(0, DeviceActionProto.getDefaultInstance())).build(),
                hub().setScenarioAdded(scenario().setAction(0, DeviceActionProto.newBuilder().setSensorId("x").setTypeValue(99))).build());
    }

    @ParameterizedTest
    @MethodSource("invalidHubs")
    void rejectsInvalidHubData(HubEventProto input) {
        assertThrows(IllegalArgumentException.class, () -> dispatcher.map(input));
    }

    @Test
    void rejectsMissingPayloadAndBlankSensorIds() {
        assertThrows(IllegalArgumentException.class, () -> dispatcher.map(sensor().build()));
        assertThrows(IllegalArgumentException.class, () -> dispatcher.map(sensor().setId(" ").setSwitchSensor(SwitchSensorProto.getDefaultInstance()).build()));
        assertThrows(IllegalArgumentException.class, () -> dispatcher.map(sensor().setHubId("").setSwitchSensor(SwitchSensorProto.getDefaultInstance()).build()));
        assertThrows(IllegalArgumentException.class, () -> new MotionSensorEventHandler().map(sensor().setSwitchSensor(SwitchSensorProto.getDefaultInstance()).build()));
    }

    @Test
    void validatesTimestampAndDefaultsToNow() {
        for (Timestamp invalid : List.of(Timestamp.newBuilder().setSeconds(-62135596801L).build(),
                Timestamp.newBuilder().setSeconds(253402300800L).build(),
                Timestamp.newBuilder().setNanos(-1).build(), Timestamp.newBuilder().setNanos(1000000000).build())) {
            assertThrows(IllegalArgumentException.class, () -> dispatcher.map(sensor().setTimestamp(invalid).setSwitchSensor(SwitchSensorProto.getDefaultInstance()).build()));
            assertThrows(IllegalArgumentException.class, () -> dispatcher.map(hub().setTimestamp(invalid).setScenarioAdded(scenario()).build()));
        }
        Instant before = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var temperature = dispatcher.map(sensor().clearTimestamp().setTemperatureSensor(TemperatureSensorProto.getDefaultInstance()).build());
        var hubResult = dispatcher.map(hub().clearTimestamp().setScenarioAdded(scenario()).build());
        Instant after = Instant.now();
        assertFalse(temperature.getTimestamp().isBefore(before));
        assertFalse(temperature.getTimestamp().isAfter(after));
        assertFalse(hubResult.getTimestamp().isBefore(before));
        assertFalse(hubResult.getTimestamp().isAfter(after));
        assertEquals(temperature.getTimestamp(), ((TemperatureSensorAvro) temperature.getPayload()).getTimestamp());
        for (long seconds : new long[]{-62135596800L, -1L, 0L, 253402300799L}) {
            var time = Timestamp.newBuilder().setSeconds(seconds).setNanos(999999999).build();
            assertEquals(Instant.ofEpochSecond(seconds, 999000000), dispatcher.map(sensor().setTimestamp(time)
                    .setSwitchSensor(SwitchSensorProto.getDefaultInstance()).build()).getTimestamp());
        }
    }

    @Test
    void rejectsIncompleteOrDuplicateRegistries() {
        assertThrows(IllegalStateException.class, () -> new EventDispatcher(List.of(), hubs()));
        assertThrows(IllegalStateException.class, () -> new EventDispatcher(sensors(), List.of()));
        var duplicateSensors = new ArrayList<>(sensors());
        duplicateSensors.add(new MotionSensorEventHandler());
        assertThrows(IllegalStateException.class, () -> new EventDispatcher(duplicateSensors, hubs()));
        var duplicateHubs = new ArrayList<>(hubs());
        duplicateHubs.add(new ScenarioAddedEventHandler());
        assertThrows(IllegalStateException.class, () -> new EventDispatcher(sensors(), duplicateHubs));
    }

    @Test
    void springDiscoversAllHandlersAndDispatcher() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.scan("ru.yandex.practicum.collector.handler");
            context.refresh();
            assertEquals(5, context.getBeansOfType(SensorEventHandler.class).size());
            assertEquals(4, context.getBeansOfType(HubEventHandler.class).size());
            assertNotNull(context.getBean(EventDispatcher.class));
        }
    }
}
