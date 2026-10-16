package ru.yandex.practicum.analyzer.analysis;

import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
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
        return scenarios.findByHubIdOrderById(hubId).stream().map(scenario -> {
            var conditionDefinitions = conditions.findByScenarioIdOrderByConditionId(scenario.getId()).stream()
                    .map(link -> new ScenarioDefinition.Condition(link.getSensor().getId(),
                            link.getCondition().getType(), link.getCondition().getOperation(),
                            link.getCondition().getValue()))
                    .toList();
            var actionDefinitions = actions.findByScenarioIdOrderByActionId(scenario.getId()).stream()
                    .map(link -> new ScenarioDefinition.Action(link.getSensor().getId(),
                            link.getAction().getType(), link.getAction().getValue()))
                    .toList();
            return new ScenarioDefinition(scenario.getHubId(), scenario.getName(),
                    conditionDefinitions, actionDefinitions);
        }).toList();
    }
}
