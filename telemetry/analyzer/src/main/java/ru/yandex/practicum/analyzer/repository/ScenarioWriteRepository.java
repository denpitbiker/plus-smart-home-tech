package ru.yandex.practicum.analyzer.repository;

import jakarta.persistence.EntityManager;
import java.util.Collection;
import java.util.ArrayList;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ConnectionCallback;
import ru.yandex.practicum.analyzer.model.ActionType;
import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.analyzer.model.ConditionOperation;
import java.util.List;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Transactional(propagation = Propagation.MANDATORY)
public class ScenarioWriteRepository {
    private final EntityManager entityManager;

    private final JdbcTemplate jdbc;
    private final int batchSize;

    public ScenarioWriteRepository(EntityManager entityManager, JdbcTemplate jdbc,
                                   @Value("${analyzer.storage.batch-size:50}") int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Storage batch size must be positive");
        }
        this.entityManager = entityManager;
        this.jdbc = jdbc;
        this.batchSize = batchSize;
    }

    public record ConditionRow(String sensorId, ConditionType type, ConditionOperation operation, Integer value) {
    }

    public record ActionRow(String sensorId, ActionType type, Integer value) {
    }

    public void insertContents(Long scenarioId, List<ConditionRow> conditions, List<ActionRow> actions) {
        // JpaTransactionManager exposes the same transaction connection to JdbcTemplate.
        entityManager.flush();
        insertRows(scenarioId, conditions, "INSERT INTO conditions(type, operation, value) VALUES (?, ?, ?)",
                "INSERT INTO scenario_conditions(scenario_id, sensor_id, condition_id) VALUES (?, ?, ?)",
                ConditionRow::sensorId, (statement, row) -> {
                    statement.setString(1, row.type().name());
                    statement.setString(2, row.operation().name());
                    statement.setObject(3, row.value(), Types.INTEGER);
                });
        insertRows(scenarioId, actions, "INSERT INTO actions(type, value) VALUES (?, ?)",
                "INSERT INTO scenario_actions(scenario_id, sensor_id, action_id) VALUES (?, ?, ?)",
                ActionRow::sensorId, (statement, row) -> {
                    statement.setString(1, row.type().name());
                    statement.setObject(2, row.value(), Types.INTEGER);
                });
        entityManager.clear();
    }

    private <T> void insertRows(Long scenarioId, List<T> rows, String insert, String linkInsert,
                                java.util.function.Function<T, String> sensorId, RowBinder<T> binder) {
        jdbc.execute((ConnectionCallback<Void>) connection -> {
            try (var statement = connection.prepareStatement(insert, new String[]{"id"});
                 var links = connection.prepareStatement(linkInsert)) {
                for (int start = 0; start < rows.size(); start += batchSize) {
                    var batch = rows.subList(start, Math.min(start + batchSize, rows.size()));
                    for (T row : batch) {
                        binder.bind(statement, row);
                        statement.addBatch();
                    }
                    statement.executeBatch();
                    // PostgreSQL returns generated keys in batch order; no per-row SELECT is needed.
                    List<Long> ids = new ArrayList<>();
                    try (var keys = statement.getGeneratedKeys()) {
                        while (keys.next()) {
                            ids.add(keys.getLong(1));
                        }
                    }
                    if (ids.size() != batch.size()) {
                        throw new SQLException("Expected one generated ID per inserted row");
                    }
                    for (int i = 0; i < batch.size(); i++) {
                        links.setLong(1, scenarioId);
                        links.setString(2, sensorId.apply(batch.get(i)));
                        links.setLong(3, ids.get(i));
                        links.addBatch();
                    }
                    links.executeBatch();
                }
            }
            return null;
        });
    }

    @FunctionalInterface
    private interface RowBinder<T> {
        void bind(PreparedStatement statement, T row) throws SQLException;
    }

    public List<Long> findScenarioIdsBySensorId(String sensorId) {
        return entityManager.createQuery("""
                select s.id from Scenario s where
                exists (select 1 from ScenarioCondition c where c.scenario = s and c.sensor.id = :sensorId)
                or exists (select 1 from ScenarioAction a where a.scenario = s and a.sensor.id = :sensorId)
                """, Long.class).setParameter("sensorId", sensorId).getResultList();
    }

    public void deleteContents(Collection<Long> scenarioIds) {
        if (scenarioIds.isEmpty()) {
            return;
        }
        entityManager.flush();
        var conditionIds = entityManager.createQuery(
                "select c.condition.id from ScenarioCondition c where c.scenario.id in :ids", Long.class)
                .setParameter("ids", scenarioIds).getResultList();
        var actionIds = entityManager.createQuery(
                "select a.action.id from ScenarioAction a where a.scenario.id in :ids", Long.class)
                .setParameter("ids", scenarioIds).getResultList();
        delete("delete from ScenarioCondition c where c.scenario.id in :ids", scenarioIds);
        delete("delete from ScenarioAction a where a.scenario.id in :ids", scenarioIds);
        delete("delete from Condition c where c.id in :ids", conditionIds);
        delete("delete from Action a where a.id in :ids", actionIds);
        // Bulk DML bypasses managed entities; stale MapsId links must never be flushed again.
        entityManager.clear();
    }

    public void deleteScenarios(Collection<Long> scenarioIds) {
        deleteContents(scenarioIds);
        delete("delete from Scenario s where s.id in :ids", scenarioIds);
        entityManager.clear();
    }

    private void delete(String query, Collection<Long> ids) {
        if (!ids.isEmpty()) {
            entityManager.createQuery(query).setParameter("ids", ids).executeUpdate();
        }
    }
}
