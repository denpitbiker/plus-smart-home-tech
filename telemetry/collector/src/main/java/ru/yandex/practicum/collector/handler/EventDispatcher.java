package ru.yandex.practicum.collector.handler;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Component
public class EventDispatcher {
    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> sensors;
    private final Map<HubEventProto.PayloadCase, HubEventHandler> hubs;

    public EventDispatcher(List<SensorEventHandler> sensorHandlers, List<HubEventHandler> hubHandlers) {
        sensors = new EnumMap<>(SensorEventProto.PayloadCase.class);
        for (var handler : sensorHandlers) {
            if (handler.getMessageType() == SensorEventProto.PayloadCase.PAYLOAD_NOT_SET
                    || sensors.putIfAbsent(handler.getMessageType(), handler) != null) {
                throw new IllegalStateException("Invalid or duplicate sensor handler: " + handler.getMessageType());
            }
        }
        hubs = new EnumMap<>(HubEventProto.PayloadCase.class);
        for (var handler : hubHandlers) {
            if (handler.getMessageType() == HubEventProto.PayloadCase.PAYLOAD_NOT_SET
                    || hubs.putIfAbsent(handler.getMessageType(), handler) != null) {
                throw new IllegalStateException("Invalid or duplicate hub handler: " + handler.getMessageType());
            }
        }
        for (var type : SensorEventProto.PayloadCase.values()) {
            if (type != SensorEventProto.PayloadCase.PAYLOAD_NOT_SET && !sensors.containsKey(type)) {
                throw new IllegalStateException("Missing sensor handler: " + type);
            }
        }
        for (var type : HubEventProto.PayloadCase.values()) {
            if (type != HubEventProto.PayloadCase.PAYLOAD_NOT_SET && !hubs.containsKey(type)) {
                throw new IllegalStateException("Missing hub handler: " + type);
            }
        }
    }

    public SensorEventAvro map(SensorEventProto event) {
        var handler = sensors.get(event.getPayloadCase());
        if (handler == null) {
            throw new IllegalArgumentException("Missing sensor payload");
        }
        return handler.map(event);
    }

    public HubEventAvro map(HubEventProto event) {
        var handler = hubs.get(event.getPayloadCase());
        if (handler == null) {
            throw new IllegalArgumentException("Missing hub payload");
        }
        return handler.map(event);
    }
}
