package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.model.ScenarioAction;
import ru.yandex.practicum.analyzer.model.ScenarioActionId;
import java.util.List;
import org.springframework.data.jpa.repository.EntityGraph;

public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {
    @EntityGraph(attributePaths = {"sensor", "action"})
    List<ScenarioAction> findByScenarioIdOrderByActionId(Long scenarioId);

}
