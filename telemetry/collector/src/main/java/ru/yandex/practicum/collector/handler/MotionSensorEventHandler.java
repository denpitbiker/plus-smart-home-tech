package ru.yandex.practicum.collector.handler;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;

@Component
public class MotionSensorEventHandler implements SensorEventHandler {
    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }

    @Override
    public SensorEventAvro map(SensorEventProto event) {
        return EventMetadata.sensor(event, getMessageType(), time -> {
            var p = event.getMotionSensor();
            return new MotionSensorAvro(p.getLinkQuality(), p.getMotion(), p.getVoltage());
        });
    }
}
