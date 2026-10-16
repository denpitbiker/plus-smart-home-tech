package ru.yandex.practicum.kafka.telemetry.serialization;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yandex.practicum.kafka.telemetry.event.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HubEventDeserializerTest {
    private final AvroSerializer serializer = new AvroSerializer();
    private final HubEventDeserializer deserializer = new HubEventDeserializer();

    static Stream<SpecificRecord> payloads() {
        return Stream.of(
                new DeviceAddedEventAvro("sensor", DeviceTypeAvro.MOTION_SENSOR),
                new DeviceRemovedEventAvro("sensor"),
                new ScenarioAddedEventAvro("scenario",
                        List.of(
                                new ScenarioConditionAvro("motion", ConditionTypeAvro.MOTION,
                                        ConditionOperationAvro.EQUALS, false),
                                new ScenarioConditionAvro("light", ConditionTypeAvro.LUMINOSITY,
                                        ConditionOperationAvro.LOWER_THAN, 20),
                                new ScenarioConditionAvro("switch", ConditionTypeAvro.SWITCH,
                                        ConditionOperationAvro.EQUALS, null)),
                        List.of(
                                new DeviceActionAvro("switch", ActionTypeAvro.ACTIVATE, null),
                                new DeviceActionAvro("switch", ActionTypeAvro.SET_VALUE, 0))),
                new ScenarioRemovedEventAvro("scenario"));
    }

    @ParameterizedTest
    @MethodSource("payloads")
    void roundTripsHubPayloadAndRejectsCorruptedBytes(SpecificRecord payload) {
        var event = new HubEventAvro("hub-1", Instant.parse("2024-08-06T15:11:24.157Z"), payload);
        byte[] bytes = serializer.serialize("hubs", event);
        assertEquals(event, deserializer.deserialize("hubs", bytes));
        for (int length = 0; length < bytes.length; length++) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            assertThrows(SerializationException.class, () -> deserializer.deserialize("hubs", truncated));
        }
        assertThrows(SerializationException.class,
                () -> deserializer.deserialize("hubs", Arrays.copyOf(bytes, bytes.length + 1)));
    }

    @Test
    void supportsNullAndRejectsInvalidUnion() {
        assertNull(deserializer.deserialize("hubs", null));
        assertThrows(SerializationException.class,
                () -> deserializer.deserialize("hubs", new byte[]{0, 0, 8}));
    }
}
