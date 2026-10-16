package ru.yandex.practicum.analyzer.analysis;

import java.time.Instant;
import ru.yandex.practicum.analyzer.model.ActionType;

public record DeviceCommand(String hubId, String scenarioName, String sensorId,
                            ActionType type, Integer value, Instant timestamp) {
}
