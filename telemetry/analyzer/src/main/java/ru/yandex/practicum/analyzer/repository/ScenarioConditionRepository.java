package ru.yandex.practicum.analyzer.repository;

import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.analyzer.model.ScenarioConditionId;

public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {
    @EntityGraph(attributePaths = {"sensor", "condition"})
    List<ScenarioCondition> findByScenarioIdInOrderByScenarioIdAscConditionIdAsc(Collection<Long> scenarioIds);
}
