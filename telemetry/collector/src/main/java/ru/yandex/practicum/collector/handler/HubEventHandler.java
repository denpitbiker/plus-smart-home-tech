package ru.yandex.practicum.collector.handler;

import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public interface HubEventHandler {
    HubEventProto.PayloadCase getMessageType();

    HubEventAvro map(HubEventProto event);
}
