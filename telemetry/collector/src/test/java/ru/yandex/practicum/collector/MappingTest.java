package ru.yandex.practicum.collector;
import java.time.Instant;
import java.util.stream.Stream;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.EnumSource;
import ru.yandex.practicum.collector.dto.*;
import ru.yandex.practicum.kafka.telemetry.serialization.AvroSerializer;
import ru.yandex.practicum.collector.mapping.*;
import ru.yandex.practicum.kafka.telemetry.event.*;
import static org.assertj.core.api.Assertions.*;
class MappingTest {
    static final JsonMapper JSON = JsonMapper.builder().findAndAddModules().build();
    static Stream<EventFixtures.Event> events() { return EventFixtures.events().stream(); }

    static SpecificRecord map(EventFixtures.Event e) throws Exception {
        return e.path().equals("sensors") ? new SensorEventMapper().map(JSON.readValue(e.json(), SensorEvent.class))
                : new HubEventMapper().map(JSON.readValue(e.json(), HubEvent.class));
    }

    @ParameterizedTest @MethodSource("events")
    void binaryRoundTrip(EventFixtures.Event event) throws Exception {
        var record = map(event);
        byte[] bytes = new AvroSerializer().serialize("topic", record);
        var decoded = new SpecificDatumReader<SpecificRecord>(record.getSchema())
                .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        assertThat(decoded).isEqualTo(record);
        assertThat(record.get(record.getSchema().getField("timestamp").pos()))
                .isEqualTo(Instant.parse("2024-08-06T15:11:24.157Z"));
    }

    @Test void mapsPayloadFieldsAndTemperatureMetadata() throws Exception {
        var records = EventFixtures.events().subList(0, 5);
        var climate = (ClimateSensorAvro) ((SensorEventAvro) map(records.get(0))).getPayload();
        assertThat(climate.getTemperatureC()).isEqualTo(21);
        assertThat(climate.getHumidity()).isEqualTo(40);
        assertThat(climate.getCo2Level()).isEqualTo(800);
        var light = (LightSensorAvro) ((SensorEventAvro) map(records.get(1))).getPayload();
        assertThat(light.getLinkQuality()).isEqualTo(75); assertThat(light.getLuminosity()).isEqualTo(59);
        var motion = (MotionSensorAvro) ((SensorEventAvro) map(records.get(2))).getPayload();
        assertThat(motion.getMotion()).isFalse(); assertThat(motion.getVoltage()).isEqualTo(12);
        var outer = (SensorEventAvro) map(records.get(4));
        var temperature = (TemperatureSensorAvro) outer.getPayload();
        assertThat(temperature.getId()).isEqualTo(outer.getId());
        assertThat(temperature.getHubId()).isEqualTo(outer.getHubId());
        assertThat(temperature.getTimestamp()).isEqualTo(outer.getTimestamp());
        assertThat(temperature.getTemperatureC()).isZero(); assertThat(temperature.getTemperatureF()).isEqualTo(32);
    }

    @Test void preservesScenarioOrderAndValueTypes() throws Exception {
        var scenario = (ScenarioAddedEventAvro) ((HubEventAvro) map(EventFixtures.events().get(7))).getPayload();
        assertThat(scenario.getConditions()).extracting(ScenarioConditionAvro::getValue)
                .containsExactly(true, 500, false, null);
        assertThat(scenario.getActions()).extracting(DeviceActionAvro::getValue).containsExactly(null, 5);
        assertThat(scenario.getActions()).extracting(DeviceActionAvro::getSensorId).containsExactly("switch-1", "switch-2");
    }

    @ParameterizedTest @EnumSource(DeviceType.class)
    void mapsAllDeviceTypes(DeviceType type) {
        var e = new DeviceAddedEvent(); e.hubId = "h"; e.id = "s"; e.deviceType = type;
        assertThat(((DeviceAddedEventAvro) new HubEventMapper().map(e).getPayload()).getType().name()).isEqualTo(type.name());
    }

    @ParameterizedTest @EnumSource(ConditionType.class)
    void mapsAllConditionTypes(ConditionType type) {
        var e = JSON.convertValue(JSON.createObjectNode().put("hubId", "h").put("type", "SCENARIO_ADDED")
                .put("name", "test"), ScenarioAddedEvent.class);
        var c = new ScenarioCondition(); c.sensorId = "s"; c.type = type; c.operation = ConditionOperation.EQUALS; c.value = 1;
        e.conditions = java.util.List.of(c); e.actions = java.util.List.of();
        var actual = ((ScenarioAddedEventAvro) new HubEventMapper().map(e).getPayload()).getConditions().getFirst();
        assertThat(actual.getType().name()).isEqualTo(type.name());
        assertThat(actual.getValue()).isEqualTo(type == ConditionType.MOTION || type == ConditionType.SWITCH ? (Object) true : 1);
    }

    @ParameterizedTest @EnumSource(ActionType.class)
    void mapsEveryAction(ActionType type) {
        var event = new ScenarioAddedEvent(); event.hubId = "h"; event.name = "test";
        var action = new DeviceAction(); action.sensorId = "s"; action.type = type; action.value = 7;
        event.actions = java.util.List.of(action); event.conditions = java.util.List.of();
        var result = (ScenarioAddedEventAvro) new HubEventMapper().map(event).getPayload();
        assertThat(result.getActions().getFirst().getType().name()).isEqualTo(type.name());
    }

    @ParameterizedTest @EnumSource(ConditionOperation.class)
    void mapsEveryOperation(ConditionOperation operation) {
        var event = new ScenarioAddedEvent(); event.hubId = "h"; event.name = "test";
        var condition = new ScenarioCondition(); condition.sensorId = "s";
        condition.type = ConditionType.HUMIDITY; condition.operation = operation; condition.value = 40;
        event.conditions = java.util.List.of(condition); event.actions = java.util.List.of();
        var result = (ScenarioAddedEventAvro) new HubEventMapper().map(event).getPayload();
        assertThat(result.getConditions().getFirst().getOperation().name()).isEqualTo(operation.name());
    }

    @Test void serializerHasNoSharedBufferAcrossConcurrentRequests() throws Exception {
        var serializer = new AvroSerializer();
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var jobs = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 100; i++) {
                var fixture = EventFixtures.events().get(i % 9);
                jobs.add(executor.submit(() -> {
                    try {
                        var record = map(fixture);
                        var bytes = serializer.serialize("topic", record);
                        var decoded = new SpecificDatumReader<SpecificRecord>(record.getSchema())
                                .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
                        assertThat(decoded).isEqualTo(record);
                    } catch (Exception e) { throw new RuntimeException(e); }
                }));
            }
            for (var job : jobs) job.get();
        }
    }
}
