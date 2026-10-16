package ru.yandex.practicum.collector.handler;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;

@Component
public class ClimateSensorEventHandler implements SensorEventHandler {
    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }

    @Override
    public SensorEventAvro map(SensorEventProto event) {
        return EventMetadata.sensor(event, getMessageType(), time -> {
            var p = event.getClimateSensor();
            return new ClimateSensorAvro(p.getTemperatureC(), p.getHumidity(), p.getCo2Level());
        });
    }
}
