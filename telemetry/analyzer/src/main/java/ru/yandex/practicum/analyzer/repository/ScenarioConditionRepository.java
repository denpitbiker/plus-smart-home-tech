package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.analyzer.model.ScenarioConditionId;
import java.util.List;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {
    @EntityGraph(attributePaths = {"sensor", "condition"})
    List<ScenarioCondition> findByScenarioIdOrderByConditionId(Long scenarioId);

    @Query("select distinct link.scenario.id from ScenarioCondition link where link.sensor.id = :sensorId")
    List<Long> findScenarioIdsBySensorId(@Param("sensorId") String sensorId);
}
