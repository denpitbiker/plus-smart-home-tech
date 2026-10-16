package ru.yandex.practicum.collector.handler;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;

@Component
public class LightSensorEventHandler implements SensorEventHandler {
    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    public SensorEventAvro map(SensorEventProto event) {
        return EventMetadata.sensor(event, getMessageType(), time -> {
            var p = event.getLightSensor();
            return new LightSensorAvro(p.getLinkQuality(), p.getLuminosity());
        });
    }
}
