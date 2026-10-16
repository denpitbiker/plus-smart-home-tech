package ru.yandex.practicum.collector.handler;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;

@Component
public class DeviceAddedEventHandler implements HubEventHandler {
    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public HubEventAvro map(HubEventProto event) {
        return EventMetadata.hub(event, getMessageType(), time -> {
            var p = event.getDeviceAdded();
            return new DeviceAddedEventAvro(EventMetadata.requireId(p.getId()), ProtoEnums.device(p.getType()));
        });
    }
}
