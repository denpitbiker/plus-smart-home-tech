package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.Table;

@Entity
@Table(name = "scenario_conditions")
public class ScenarioCondition {
    @EmbeddedId
    private ScenarioConditionId id;

    @MapsId("scenarioId")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "scenario_id")
    private Scenario scenario;

    @MapsId("sensorId")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "sensor_id")
    private Sensor sensor;

    @MapsId("conditionId")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "condition_id")
    private Condition condition;

    protected ScenarioCondition() {
    }

    public ScenarioCondition(Scenario scenario, Sensor sensor, Condition condition) {
        this.id = new ScenarioConditionId(scenario.getId(), sensor.getId(), condition.getId());
        this.scenario = scenario;
        this.sensor = sensor;
        this.condition = condition;
    }

    public Scenario getScenario() {
        return scenario;
    }

    public Sensor getSensor() {
        return sensor;
    }

    public Condition getCondition() {
        return condition;
    }
}
