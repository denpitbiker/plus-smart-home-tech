package ru.yandex.practicum.collector.mapping;
import java.time.temporal.ChronoUnit;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.*;
import ru.yandex.practicum.kafka.telemetry.event.*;
@Component
public class SensorEventMapper {
    public SensorEventAvro map(SensorEvent event) {
        var time = event.timestamp.truncatedTo(ChronoUnit.MILLIS);
        Object payload = switch (event) {
            case ClimateSensorEvent e -> new ClimateSensorAvro(e.temperatureC, e.humidity, e.co2Level);
            case LightSensorEvent e -> new LightSensorAvro(e.linkQuality, e.luminosity);
            case MotionSensorEvent e -> new MotionSensorAvro(e.linkQuality, e.motion, e.voltage);
            case SwitchSensorEvent e -> new SwitchSensorAvro(e.state);
            case TemperatureSensorEvent e -> new TemperatureSensorAvro(e.id, e.hubId, time, e.temperatureC, e.temperatureF);
            default -> throw new IllegalArgumentException("Unsupported sensor event: " + event.getType());
        };
        return new SensorEventAvro(event.id, event.hubId, time, payload);
    }
}
