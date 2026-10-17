package ru.yandex.practicum.collector;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public final class GrpcEventFixtures {
    public static final Instant TIME = Instant.parse("2024-08-06T15:11:24.157Z");
    private static final Timestamp TS = Timestamp.newBuilder().setSeconds(TIME.getEpochSecond())
            .setNanos(157123000).build();

    private GrpcEventFixtures() {
    }

    public record Event(Message request, SpecificRecord expected) {
        public boolean sensor() {
            return request instanceof SensorEventProto;
        }
    }

    private static Event sensor(SensorEventProto.Builder builder, Object payload) {
        return new Event(builder.setId("sensor-1").setHubId("hub-1").setTimestamp(TS).build(),
                new SensorEventAvro("sensor-1", "hub-1", TIME, payload));
    }

    private static Event hub(HubEventProto.Builder builder, Object payload) {
        return new Event(builder.setHubId("hub-2").setTimestamp(TS).build(), new HubEventAvro("hub-2", TIME, payload));
    }

    public static List<Event> events() {
        var scenario = ScenarioAddedEventProto.newBuilder().setName("Автосвет")
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("motion-1").setType(ConditionTypeProto.MOTION).setBoolValue(true))
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("light-1").setType(ConditionTypeProto.LUMINOSITY)
                        .setOperation(ConditionOperationProto.LOWER_THAN).setIntValue(500))
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("switch-1").setType(ConditionTypeProto.SWITCH).setBoolValue(false))
                .addCondition(ScenarioConditionProto.newBuilder().setSensorId("temp-1").setType(ConditionTypeProto.TEMPERATURE))
                .addAction(DeviceActionProto.newBuilder().setSensorId("switch-1").setType(ActionTypeProto.ACTIVATE))
                .addAction(DeviceActionProto.newBuilder().setSensorId("switch-2").setType(ActionTypeProto.SET_VALUE).setValue(5));
        return List.of(
                sensor(SensorEventProto.newBuilder().setClimateSensor(ClimateSensorProto.newBuilder()
                        .setTemperatureC(21).setHumidity(40).setCo2Level(800)), new ClimateSensorAvro(21, 40, 800)),
                sensor(SensorEventProto.newBuilder().setLightSensor(LightSensorProto.newBuilder()
                        .setLinkQuality(75).setLuminosity(59)), new LightSensorAvro(75, 59)),
                sensor(SensorEventProto.newBuilder().setMotionSensor(MotionSensorProto.newBuilder()
                        .setLinkQuality(70).setMotion(false).setVoltage(12)), new MotionSensorAvro(70, false, 12)),
                sensor(SensorEventProto.newBuilder().setSwitchSensor(SwitchSensorProto.newBuilder().setState(false)), new SwitchSensorAvro(false)),
                sensor(SensorEventProto.newBuilder().setTemperatureSensor(TemperatureSensorProto.newBuilder()
                        .setTemperatureC(0).setTemperatureF(32)), new TemperatureSensorAvro("sensor-1", "hub-1", TIME, 0, 32)),
                hub(HubEventProto.newBuilder().setDeviceAdded(DeviceAddedEventProto.newBuilder()
                        .setId("switch-1").setType(DeviceTypeProto.SWITCH_SENSOR)), new DeviceAddedEventAvro("switch-1", DeviceTypeAvro.SWITCH_SENSOR)),
                hub(HubEventProto.newBuilder().setDeviceRemoved(DeviceRemovedEventProto.newBuilder().setId("switch-1")), new DeviceRemovedEventAvro("switch-1")),
                hub(HubEventProto.newBuilder().setScenarioAdded(scenario), new ScenarioAddedEventAvro("Автосвет",
                        List.of(new ScenarioConditionAvro("motion-1", ConditionTypeAvro.MOTION, ConditionOperationAvro.EQUALS, true),
                                new ScenarioConditionAvro("light-1", ConditionTypeAvro.LUMINOSITY, ConditionOperationAvro.LOWER_THAN, 500),
                                new ScenarioConditionAvro("switch-1", ConditionTypeAvro.SWITCH, ConditionOperationAvro.EQUALS, false),
                                new ScenarioConditionAvro("temp-1", ConditionTypeAvro.TEMPERATURE, ConditionOperationAvro.EQUALS, null)),
                        List.of(new DeviceActionAvro("switch-1", ActionTypeAvro.ACTIVATE, null), new DeviceActionAvro("switch-2", ActionTypeAvro.SET_VALUE, 5)))),
                hub(HubEventProto.newBuilder().setScenarioRemoved(ScenarioRemovedEventProto.newBuilder().setName("Автосвет")), new ScenarioRemovedEventAvro("Автосвет")));
    }
}
