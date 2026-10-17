package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class ScenarioActionId implements Serializable {
    @Column(name = "scenario_id")
    private Long scenarioId;
    @Column(name = "sensor_id")
    private String sensorId;
    @Column(name = "action_id")
    private Long actionId;

    protected ScenarioActionId() {
    }

    public ScenarioActionId(Long scenarioId, String sensorId, Long actionId) {
        this.scenarioId = scenarioId;
        this.sensorId = sensorId;
        this.actionId = actionId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ScenarioActionId that)) {
            return false;
        }
        return Objects.equals(scenarioId, that.scenarioId)
                && Objects.equals(sensorId, that.sensorId)
                && Objects.equals(actionId, that.actionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scenarioId, sensorId, actionId);
    }
}
