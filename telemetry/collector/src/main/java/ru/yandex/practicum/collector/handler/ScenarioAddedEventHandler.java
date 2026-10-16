package ru.yandex.practicum.collector.handler;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

@Component
public class ScenarioAddedEventHandler implements HubEventHandler {
    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public HubEventAvro map(HubEventProto event) {
        return EventMetadata.hub(event, getMessageType(), time -> {
            var p = event.getScenarioAdded();
            EventMetadata.scenarioName(p.getName());
            if (p.getConditionCount() == 0 || p.getActionCount() == 0) {
                throw new IllegalArgumentException("Scenario requires conditions and actions");
            }
            return new ScenarioAddedEventAvro(p.getName(),
                    p.getConditionList().stream().map(this::condition).toList(),
                    p.getActionList().stream().map(this::action).toList());
        });
    }

    private ScenarioConditionAvro condition(ScenarioConditionProto p) {
        var type = ProtoEnums.condition(p.getType());
        boolean logical = type == ConditionTypeAvro.MOTION || type == ConditionTypeAvro.SWITCH;
        Object value = switch (p.getValueCase()) {
            case VALUE_NOT_SET -> null;
            case BOOL_VALUE -> {
                if (!logical) {
                    throw new IllegalArgumentException("Numeric condition requires int_value");
                }
                yield p.getBoolValue();
            }
            case INT_VALUE -> {
                if (logical) {
                    throw new IllegalArgumentException("Logical condition requires bool_value");
                }
                yield p.getIntValue();
            }
        };
        return new ScenarioConditionAvro(EventMetadata.requireId(p.getSensorId()), type,
                ProtoEnums.operation(p.getOperation()), value);
    }

    private DeviceActionAvro action(DeviceActionProto p) {
        return new DeviceActionAvro(EventMetadata.requireId(p.getSensorId()), ProtoEnums.action(p.getType()),
                p.hasValue() ? p.getValue() : null);
    }
}
