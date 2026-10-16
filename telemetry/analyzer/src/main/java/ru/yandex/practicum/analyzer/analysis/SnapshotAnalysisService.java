package ru.yandex.practicum.analyzer.analysis;

import java.util.List;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Service
public class SnapshotAnalysisService {
    private final ScenarioLoader loader;
    private final ScenarioEvaluator evaluator;

    public SnapshotAnalysisService(ScenarioLoader loader, ScenarioEvaluator evaluator) {
        this.loader = loader;
        this.evaluator = evaluator;
    }

    public List<DeviceCommand> analyze(SensorsSnapshotAvro snapshot) {
        ScenarioEvaluator.validate(snapshot);
        return evaluator.evaluate(snapshot, loader.load(snapshot.getHubId()));
    }
}
