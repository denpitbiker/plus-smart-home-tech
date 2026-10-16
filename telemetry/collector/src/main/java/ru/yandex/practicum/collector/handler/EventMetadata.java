package ru.yandex.practicum.collector.handler;

import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

final class EventMetadata {
    private EventMetadata() {
    }

    static SensorEventAvro sensor(SensorEventProto event, SensorEventProto.PayloadCase expected,
                                 Function<Instant, Object> payload) {
        if (event.getPayloadCase() != expected) {
            throw new IllegalArgumentException("Unexpected sensor payload: " + event.getPayloadCase());
        }
        requireId(event.getId());
        requireId(event.getHubId());
        Instant time = timestamp(event.hasTimestamp(), event.getTimestamp());
        return new SensorEventAvro(event.getId(), event.getHubId(), time, payload.apply(time));
    }

    static HubEventAvro hub(HubEventProto event, HubEventProto.PayloadCase expected,
                           Function<Instant, Object> payload) {
        if (event.getPayloadCase() != expected) {
            throw new IllegalArgumentException("Unexpected hub payload: " + event.getPayloadCase());
        }
        requireId(event.getHubId());
        Instant time = timestamp(event.hasTimestamp(), event.getTimestamp());
        return new HubEventAvro(event.getHubId(), time, payload.apply(time));
    }

    static String requireId(String value) {
        if (value.isBlank()) {
            throw new IllegalArgumentException("Identifier must not be blank");
        }
        return value;
    }

    static String scenarioName(String value) {
        if (value.isBlank() || value.length() < 3) {
            throw new IllegalArgumentException("Scenario name must contain at least three characters");
        }
        return value;
    }

    private static Instant timestamp(boolean present, Timestamp value) {
        if (!present) {
            return Instant.now().truncatedTo(ChronoUnit.MILLIS);
        }
        // Protobuf Timestamp covers 0001-01-01 through 9999-12-31, with nonnegative nanos.
        if (value.getSeconds() < -62135596800L || value.getSeconds() > 253402300799L
                || value.getNanos() < 0 || value.getNanos() > 999999999) {
            throw new IllegalArgumentException("Timestamp is outside the Protobuf range");
        }
        return Instant.ofEpochSecond(value.getSeconds(), value.getNanos()).truncatedTo(ChronoUnit.MILLIS);
    }
}
