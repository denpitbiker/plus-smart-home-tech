package ru.yandex.practicum.aggregator.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.specific.SpecificData;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

/** In-memory state owned by the single consumer poll-loop thread. */
@Component
public class SnapshotStore {
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        validate(event);
        var snapshot = snapshots.get(event.getHubId());
        if (snapshot != null) {
            var oldState = snapshot.getSensorsState().get(event.getId());
            if (oldState != null && (oldState.getTimestamp().isAfter(event.getTimestamp())
                    || oldState.getData().equals(event.getPayload()))) {
                return Optional.empty();
            }
        }

        // Own the payload as well as the map: callers may reuse mutable Avro records.
        var state = SpecificData.get().deepCopy(SensorStateAvro.getClassSchema(),
                new SensorStateAvro(event.getTimestamp(), event.getPayload()));
        if (snapshot == null) {
            snapshot = new SensorsSnapshotAvro(event.getHubId(), event.getTimestamp(), new HashMap<>());
            snapshots.put(event.getHubId(), snapshot);
        }
        snapshot.getSensorsState().put(event.getId(), state);
        snapshot.setTimestamp(event.getTimestamp());
        return Optional.of(SpecificData.get().deepCopy(SensorsSnapshotAvro.getClassSchema(), snapshot));
    }

    private void validate(SensorEventAvro event) {
        if (event == null) {
            throw new IllegalArgumentException("Sensor event must not be null");
        }
        if (event.getHubId() == null || event.getHubId().isBlank()) {
            throw new IllegalArgumentException("Hub ID must not be blank");
        }
        if (event.getId() == null || event.getId().isBlank()) {
            throw new IllegalArgumentException("Sensor ID must not be blank");
        }
        if (event.getTimestamp() == null || event.getPayload() == null) {
            throw new IllegalArgumentException("Timestamp and payload are required");
        }
    }
}
