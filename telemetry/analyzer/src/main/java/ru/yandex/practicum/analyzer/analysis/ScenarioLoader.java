package ru.yandex.practicum.analyzer.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;

@Service
public class ScenarioLoader {
    private final ScenarioRepository scenarios;
    private final ScenarioConditionRepository conditions;
    private final ScenarioActionRepository actions;

    public ScenarioLoader(ScenarioRepository scenarios, ScenarioConditionRepository conditions,
                          ScenarioActionRepository actions) {
        this.scenarios = scenarios;
        this.conditions = conditions;
        this.actions = actions;
    }

    // All queries see the same configuration even if a hub event changes it concurrently.
    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public List<ScenarioDefinition> load(String hubId) {
        var hubScenarios = scenarios.findByHubIdOrderById(hubId);
        if (hubScenarios.isEmpty()) {
            return List.of();
        }
        var scenarioIds = hubScenarios.stream().map(Scenario::getId).toList();
        Map<Long, List<ScenarioDefinition.Condition>> conditionsByScenario = new HashMap<>();
        for (var link : conditions.findByScenarioIdInOrderByScenarioIdAscConditionIdAsc(scenarioIds)) {
            conditionsByScenario.computeIfAbsent(link.getScenario().getId(), ignored -> new ArrayList<>())
                    .add(new ScenarioDefinition.Condition(link.getSensor().getId(),
                            link.getCondition().getType(), link.getCondition().getOperation(),
                            link.getCondition().getValue()));
        }
        Map<Long, List<ScenarioDefinition.Action>> actionsByScenario = new HashMap<>();
        for (var link : actions.findByScenarioIdInOrderByScenarioIdAscActionIdAsc(scenarioIds)) {
            actionsByScenario.computeIfAbsent(link.getScenario().getId(), ignored -> new ArrayList<>())
                    .add(new ScenarioDefinition.Action(link.getSensor().getId(),
                            link.getAction().getType(), link.getAction().getValue()));
        }
        return hubScenarios.stream().map(scenario -> new ScenarioDefinition(
                scenario.getHubId(), scenario.getName(),
                conditionsByScenario.getOrDefault(scenario.getId(), List.of()),
                actionsByScenario.getOrDefault(scenario.getId(), List.of()))).toList();
    }
}
