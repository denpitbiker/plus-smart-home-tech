package ru.yandex.practicum.analyzer.repository;

import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.model.ScenarioAction;
import ru.yandex.practicum.analyzer.model.ScenarioActionId;

public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {
    @EntityGraph(attributePaths = {"sensor", "action"})
    List<ScenarioAction> findByScenarioIdInOrderByScenarioIdAscActionIdAsc(Collection<Long> scenarioIds);
}
