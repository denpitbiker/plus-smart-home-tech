package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class ScenarioConditionId implements Serializable {
    @Column(name = "scenario_id")
    private Long scenarioId;
    @Column(name = "sensor_id")
    private String sensorId;
    @Column(name = "condition_id")
    private Long conditionId;

    protected ScenarioConditionId() {
    }

    public ScenarioConditionId(Long scenarioId, String sensorId, Long conditionId) {
        this.scenarioId = scenarioId;
        this.sensorId = sensorId;
        this.conditionId = conditionId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ScenarioConditionId that)) {
            return false;
        }
        return Objects.equals(scenarioId, that.scenarioId)
                && Objects.equals(sensorId, that.sensorId)
                && Objects.equals(conditionId, that.conditionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scenarioId, sensorId, conditionId);
    }
}
