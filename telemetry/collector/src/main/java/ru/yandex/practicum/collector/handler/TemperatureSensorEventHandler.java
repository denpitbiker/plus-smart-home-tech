package ru.yandex.practicum.collector.handler;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

@Component
public class TemperatureSensorEventHandler implements SensorEventHandler {
    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
    }

    @Override
    public SensorEventAvro map(SensorEventProto event) {
        return EventMetadata.sensor(event, getMessageType(), time -> {
            var p = event.getTemperatureSensor();
            return new TemperatureSensorAvro(event.getId(), event.getHubId(), time, p.getTemperatureC(), p.getTemperatureF());
        });
    }
}
