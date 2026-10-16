package ru.yandex.practicum.collector.mapping;
import java.time.temporal.ChronoUnit;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.*;
import ru.yandex.practicum.kafka.telemetry.event.*;
@Component
public class HubEventMapper {
    public HubEventAvro map(HubEvent event) {
        Object payload = switch (event) {
            case DeviceAddedEvent e -> new DeviceAddedEventAvro(e.id, DeviceTypeAvro.valueOf(e.deviceType.name()));
            case DeviceRemovedEvent e -> new DeviceRemovedEventAvro(e.id);
            case ScenarioAddedEvent e -> new ScenarioAddedEventAvro(e.name,
                    e.conditions.stream().map(this::condition).toList(),
                    e.actions.stream().map(this::action).toList());
            case ScenarioRemovedEvent e -> new ScenarioRemovedEventAvro(e.name);
            default -> throw new IllegalArgumentException("Unsupported hub event: " + event.getType());
        };
        return new HubEventAvro(event.hubId, event.timestamp.truncatedTo(ChronoUnit.MILLIS), payload);
    }

    private ScenarioConditionAvro condition(ScenarioCondition c) {
        Object value = c.value;
        if (value != null && (c.type == ConditionType.MOTION || c.type == ConditionType.SWITCH)) {
            value = c.value == 1;
        }
        return new ScenarioConditionAvro(c.sensorId, ConditionTypeAvro.valueOf(c.type.name()),
                ConditionOperationAvro.valueOf(c.operation.name()), value);
    }

    private DeviceActionAvro action(DeviceAction a) {
        return new DeviceActionAvro(a.sensorId, ActionTypeAvro.valueOf(a.type.name()), a.value);
    }
}
