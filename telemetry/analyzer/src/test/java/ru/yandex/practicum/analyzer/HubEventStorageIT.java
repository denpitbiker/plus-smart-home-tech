package ru.yandex.practicum.analyzer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.practicum.analyzer.service.HubEventService;
import ru.yandex.practicum.analyzer.analysis.ScenarioLoader;
import ru.yandex.practicum.analyzer.analysis.ScenarioEvaluator;
import ru.yandex.practicum.analyzer.analysis.SnapshotAnalysisService;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HubEventStorageIT {
    private final String url = System.getProperty("test.postgres.url", "jdbc:postgresql://localhost:5432/telemetry_analyzer");
    private final String username = System.getProperty("test.postgres.username", "dbuser");
    private final String password = System.getProperty("test.postgres.password", "12345");
    private final String schema = "analyzer_it_" + UUID.randomUUID().toString().replace("-", "");
    private ConfigurableApplicationContext context;
    private JdbcTemplate jdbc;
    private HubEventService service;

    @BeforeEach
    void start() throws Exception {
        try (Connection connection = DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        openContext();
    }

    private void openContext() {
        context = new SpringApplicationBuilder(Analyzer.class, JdbcProbe.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + username,
                "--spring.datasource.password=" + password,
                "--spring.datasource.hikari.schema=" + schema,
                "--spring.flyway.default-schema=" + schema,
                "--spring.flyway.schemas=" + schema,
                "--spring.jpa.properties.hibernate.default_schema=" + schema,
                "--spring.jpa.properties.hibernate.generate_statistics=true",
                "--spring.main.banner-mode=off");
        jdbc = context.getBean(JdbcTemplate.class);
        service = context.getBean(HubEventService.class);
    }

    @AfterEach
    void stop() throws Exception {
        try {
            if (context != null) {
                context.close();
            }
        } finally {
            try (Connection connection = DriverManager.getConnection(url, username, password);
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }
    }

    @Test
    void registersIdempotentlyAndRejectsCrossHubId() {
        add("hub-a", "sensor");
        add("hub-a", "sensor");
        assertThrows(IllegalArgumentException.class, () -> add("hub-b", "sensor"));
        send("hub-b", new DeviceRemovedEventAvro("sensor"));
        assertEquals(1, count("sensors"));
        assertEquals("hub-a", jdbc.queryForObject("SELECT hub_id FROM sensors", String.class));
        send("hub-a", new DeviceRemovedEventAvro("sensor"));
        send("hub-a", new DeviceRemovedEventAvro("sensor"));
        assertEquals(0, count("sensors"));
    }

    @Test
    void replacesScenarioWithoutOrphansAndPreservesValuesAndActionOrder() {
        add("hub", "sensor");
        var payload = new ScenarioAddedEventAvro("scenario", List.of(
                condition(ConditionTypeAvro.MOTION, false),
                condition(ConditionTypeAvro.SWITCH, true),
                condition(ConditionTypeAvro.TEMPERATURE, 0),
                condition(ConditionTypeAvro.HUMIDITY, null)),
                List.of(action(ActionTypeAvro.SET_VALUE, 0),
                        action(ActionTypeAvro.INVERSE, null)));
        send("hub", payload);
        Long id = jdbc.queryForObject("SELECT id FROM scenarios", Long.class);
        assertEquals(Arrays.asList(0, 1, 0, null),
                jdbc.query("SELECT value FROM conditions ORDER BY id", (rs, row) -> (Integer) rs.getObject(1)));
        assertEquals(List.of("SET_VALUE", "INVERSE"),
                jdbc.queryForList("SELECT type FROM actions ORDER BY id", String.class));
        assertEquals(Arrays.asList(0, null),
                jdbc.query("SELECT value FROM actions ORDER BY id", (rs, row) -> (Integer) rs.getObject(1)));
        send("hub", payload);
        assertEquals(id, jdbc.queryForObject("SELECT id FROM scenarios", Long.class));
        assertEquals(4, count("conditions"));
        assertEquals(2, count("actions"));
        send("hub", scenario("scenario", "sensor", "sensor"));
        assertEquals(1, count("scenarios"));
        assertEquals(1, count("conditions"));
        assertEquals(1, count("actions"));
        assertEquals(1, count("scenario_conditions"));
        assertEquals(1, count("scenario_actions"));
        send("hub", new ScenarioRemovedEventAvro("scenario"));
        send("hub", new ScenarioRemovedEventAvro("scenario"));
        assertNoScenarioRows();
        assertEquals(1, count("sensors"));
    }

    @Test
    void checksEveryReferenceBeforeReplacingExistingScenario() {
        add("hub", "sensor");
        add("other", "foreign");
        send("hub", scenario("scenario", "sensor", "sensor"));
        for (String invalid : List.of("missing", "foreign")) {
            var payload = scenario("scenario", "sensor", invalid);
            assertThrows(IllegalArgumentException.class, () -> send("hub", payload));
            var invalidCondition = scenario("scenario", invalid, "sensor");
            assertThrows(IllegalArgumentException.class, () -> send("hub", invalidCondition));
        }
        assertEquals(1, count("scenarios"));
        assertEquals(1, count("conditions"));
        assertEquals(1, count("actions"));
    }

    @Test
    void rollsBackPartialReplacementOnDatabaseFailure() {
        add("hub", "sensor");
        send("hub", scenario("scenario", "sensor", "sensor"));
        var before = jdbc.queryForList("SELECT * FROM conditions");
        jdbc.execute("ALTER TABLE actions ADD CONSTRAINT test_value CHECK (value IS NULL OR value <> 99999)");
        var replacement = scenario("scenario", "sensor", "sensor");
        replacement.setActions(List.of(action(ActionTypeAvro.ACTIVATE, null), action(ActionTypeAvro.SET_VALUE, 99999)));
        assertThrows(RuntimeException.class, () -> send("hub", replacement));
        assertEquals(before, jdbc.queryForList("SELECT * FROM conditions"));
        assertEquals(1, count("actions"));
        assertEquals(1, count("scenario_conditions"));
        assertEquals(1, count("scenario_actions"));
    }

    @Test
    void deletesScenariosReferencingDeviceInEitherRoleWithoutAffectingOtherHubs() {
        add("hub", "sensor");
        add("hub", "other");
        add("second", "second-sensor");
        send("hub", scenario("condition-reference", "sensor", "other"));
        send("hub", scenario("action-reference", "other", "sensor"));
        send("hub", scenario("unaffected", "other", "other"));
        send("second", scenario("unaffected", "second-sensor", "second-sensor"));
        send("hub", new DeviceRemovedEventAvro("sensor"));
        send("hub", new DeviceRemovedEventAvro("sensor"));
        assertEquals(2, count("sensors"));
        for (String table : List.of("scenarios", "conditions", "actions", "scenario_conditions", "scenario_actions")) {
            assertEquals(2, count(table), table);
        }
        assertEquals(List.of("hub", "second"), jdbc.queryForList("SELECT hub_id FROM scenarios ORDER BY hub_id", String.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"conditions", "actions"})
    void databaseRejectsCrossHubLinksOnInsertAndUpdate(String detail) {
        add("hub", "sensor");
        add("other", "foreign");
        send("hub", scenario("scenario", "sensor", "sensor"));
        String id = detail.equals("conditions") ? "condition_id" : "action_id";
        String table = "scenario_" + detail;
        assertThrows(RuntimeException.class, () -> jdbc.update("INSERT INTO " + table
                + " (scenario_id, sensor_id, " + id + ") SELECT scenario_id, 'foreign', " + id + " FROM " + table));
        assertThrows(RuntimeException.class, () -> jdbc.update("UPDATE " + table + " SET sensor_id='foreign'"));
        assertEquals("sensor", jdbc.queryForObject("SELECT sensor_id FROM " + table, String.class));
    }

    @Test
    void databaseEnforcesForeignKeysUniquenessAndRequiredFields() {
        add("hub", "sensor");
        send("hub", scenario("scenario", "sensor", "sensor"));
        assertThrows(RuntimeException.class, () -> jdbc.update("INSERT INTO scenarios(hub_id,name) VALUES ('hub','scenario')"));
        assertThrows(RuntimeException.class, () -> jdbc.update("INSERT INTO scenarios(hub_id,name) VALUES (NULL,'another')"));
        assertThrows(RuntimeException.class, () -> jdbc.update("INSERT INTO sensors(id,hub_id) VALUES ('sensor','other')"));
        assertThrows(RuntimeException.class, () -> jdbc.update("INSERT INTO conditions(type,operation) VALUES (NULL,'EQUALS')"));
        assertThrows(RuntimeException.class, () -> jdbc.update("INSERT INTO actions(type) VALUES (NULL)"));
        assertThrows(RuntimeException.class, () -> jdbc.update("DELETE FROM sensors WHERE id='sensor'"));
        assertThrows(RuntimeException.class, () -> jdbc.update("UPDATE scenario_conditions SET condition_id=-1"));
    }

    @Test
    void retainsConfigurationWhenApplicationContextRestarts() {
        add("hub", "sensor");
        send("hub", scenario("scenario", "sensor", "sensor"));
        var before = jdbc.queryForList("SELECT * FROM scenarios");
        context.close();
        openContext();
        assertEquals(before, jdbc.queryForList("SELECT * FROM scenarios"));
        assertEquals(1, count("sensors"));
        assertEquals(1, count("conditions"));
        assertEquals(1, count("actions"));
        assertNotNull(service);
        send("hub", new ScenarioRemovedEventAvro("scenario"));
        assertNoScenarioRows();
    }

    @Test
    void rejectsMalformedEventsWithoutWrites() {
        assertThrows(IllegalArgumentException.class, () -> service.handle(null));
        assertThrows(IllegalArgumentException.class, () -> send(" ", new DeviceRemovedEventAvro("sensor")));
        assertThrows(IllegalArgumentException.class, () -> send("hub", null));
        var missingTimestamp = new HubEventAvro();
        missingTimestamp.setHubId("hub");
        missingTimestamp.setPayload(new DeviceRemovedEventAvro("sensor"));
        assertThrows(IllegalArgumentException.class, () -> service.handle(missingTimestamp));
        add("hub", "sensor");
        var invalid = new ArrayList<ScenarioAddedEventAvro>();
        invalid.add(scenario("ab", "sensor", "sensor"));
        invalid.add(new ScenarioAddedEventAvro("scenario", List.of(), List.of(action(ActionTypeAvro.ACTIVATE, null))));
        invalid.add(new ScenarioAddedEventAvro("scenario", List.of(condition(ConditionTypeAvro.MOTION, true)), List.of()));
        invalid.add(new ScenarioAddedEventAvro("scenario", List.of(condition(ConditionTypeAvro.MOTION, 1)),
                List.of(action(ActionTypeAvro.ACTIVATE, null))));
        invalid.add(new ScenarioAddedEventAvro("scenario", List.of(condition(ConditionTypeAvro.TEMPERATURE, true)),
                List.of(action(ActionTypeAvro.ACTIVATE, null))));
        for (var event : invalid) {
            assertThrows(IllegalArgumentException.class, () -> send("hub", event));
        }
        assertNoScenarioRows();
    }

    @Test
    void loadsOrderedDetachedDefinitionsAndRefreshesOnEachSnapshot() {
        add("hub", "sensor");
        add("hub", "target");
        add("other", "foreign");
        var first = scenario("zzz-first", "sensor", "target");
        first.setActions(List.of(new DeviceActionAvro("target", ActionTypeAvro.INVERSE, null),
                new DeviceActionAvro("target", ActionTypeAvro.SET_VALUE, 0)));
        send("hub", first);
        send("hub", scenario("aaa-second", "sensor", "target"));
        send("other", scenario("foreign", "foreign", "foreign"));
        var loader = context.getBean(ScenarioLoader.class);
        var definitions = loader.load("hub");
        assertEquals(List.of("zzz-first", "aaa-second"), definitions.stream().map(scene -> scene.name()).toList());
        assertEquals(List.of("INVERSE", "SET_VALUE"),
                definitions.getFirst().actions().stream().map(action -> action.type().name()).toList());
        var time = Instant.parse("2024-08-06T15:11:24.157Z");
        var snapshot = new SensorsSnapshotAvro("hub", time,
                Map.of("sensor", new SensorStateAvro(time, new SwitchSensorAvro(true))));
        var analysis = context.getBean(SnapshotAnalysisService.class);
        assertEquals(3, analysis.analyze(snapshot).size());
        send("hub", new ScenarioRemovedEventAvro("zzz-first"));
        assertEquals(1, analysis.analyze(snapshot).size());
        assertEquals(List.of(), loader.load("unknown"));

        context.close();
        var commands = new ScenarioEvaluator().evaluate(snapshot, definitions);
        assertEquals(3, commands.size());
        assertEquals("target", commands.getFirst().sensorId());
        assertEquals(time, commands.getFirst().timestamp());
        assertThrows(UnsupportedOperationException.class, () -> definitions.getFirst().actions().clear());
    }

    @Test
    void batchesLargeScenariosAndKeepsIdentityIdsOrderedAcrossRestart() {
        add("hub", "sensor");
        var conditions = java.util.stream.IntStream.range(0, 125)
                .mapToObj(i -> condition(ConditionTypeAvro.TEMPERATURE, i)).toList();
        var actions = java.util.stream.IntStream.range(0, 125)
                .mapToObj(i -> action(ActionTypeAvro.SET_VALUE, i)).toList();
        add("hub", "second");
        add("hub", "third");
        var sensorIds = List.of("sensor", "second", "third");
        for (int i = 0; i < 125; i++) {
            conditions.get(i).setSensorId(sensorIds.get(i % 3));
            actions.get(i).setSensorId(sensorIds.get(i % 3));
        }
        var stats = context.getBean(jakarta.persistence.EntityManagerFactory.class)
                .unwrap(org.hibernate.SessionFactory.class).getStatistics();
        stats.clear();
        JdbcProbe.reset();
        send("hub", new ScenarioAddedEventAvro("large", conditions, actions));
        System.out.printf("Batch measurement: 125 conditions + 125 actions + 250 links, batches=%d, flushes=%d%n",
                JdbcProbe.batches, stats.getFlushCount());
        assertEquals(12, JdbcProbe.batches); // Four entity types, three batches each.
        assertTrue(stats.getFlushCount() <= 3, "Flushes must be phase-based");
        assertEquals(0, JdbcProbe.executions.stream().filter(sql -> sql.startsWith("select")
                && (sql.contains("scenario_conditions") || sql.contains("scenario_actions"))).count());
        assertEquals(java.util.stream.IntStream.range(0, 125).boxed().toList(),
                jdbc.queryForList("SELECT value FROM actions ORDER BY id", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM scenario_conditions sc "
                + "JOIN conditions c ON c.id=sc.condition_id WHERE sc.sensor_id <> "
                + "CASE c.value % 3 WHEN 0 THEN 'sensor' WHEN 1 THEN 'second' ELSE 'third' END", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM scenario_actions sa "
                + "JOIN actions a ON a.id=sa.action_id WHERE sa.sensor_id <> "
                + "CASE a.value % 3 WHEN 0 THEN 'sensor' WHEN 1 THEN 'second' ELSE 'third' END", Integer.class));
        Long previousMax = jdbc.queryForObject("SELECT max(id) FROM actions", Long.class);
        context.close();
        openContext();
        send("hub", new ScenarioAddedEventAvro("after-restart", conditions, actions));
        assertEquals(250, count("actions"));
        assertTrue(jdbc.queryForObject("SELECT min(a.id) FROM actions a JOIN scenario_actions sa ON sa.action_id=a.id "
                + "JOIN scenarios s ON s.id=sa.scenario_id WHERE s.name='after-restart'", Long.class) > previousMax);
        send("hub", new ScenarioAddedEventAvro("large", conditions, actions));
        assertEquals(250, count("actions"));
        send("hub", new DeviceRemovedEventAvro("sensor"));
        assertNoScenarioRows();
    }

    @Test
    void rollsBackJdbcLinksAndJpaReplacementTogether() {
        add("hub", "sensor");
        send("hub", scenario("scenario", "sensor", "sensor"));
        var before = jdbc.queryForList("SELECT * FROM scenario_actions");
        jdbc.execute("ALTER TABLE scenario_actions ADD CONSTRAINT reject_new_action CHECK (action_id < 2)");
        assertThrows(RuntimeException.class, () -> send("hub", scenario("scenario", "sensor", "sensor")));
        assertEquals(before, jdbc.queryForList("SELECT * FROM scenario_actions"));
        assertEquals(1, count("conditions"));
        assertEquals(1, count("actions"));
        assertEquals(1, count("scenario_conditions"));
        assertEquals(1, count("scenarios"));
    }

    @Test
    void deletionQueryCountDoesNotGrowWithScenarioCount() {
        int single = deleteScenariosAndCountQueries(1);
        int many = deleteScenariosAndCountQueries(20);
        System.out.printf("Deletion JDBC executions: 1 scenario=%d, 20 scenarios=%d%n", single, many);
        assertEquals(single, many);
        assertNoScenarioRows();
    }

    private int deleteScenariosAndCountQueries(int number) {
        add("hub", "sensor");
        for (int i = 0; i < number; i++) {
            send("hub", scenario("scenario-" + i, "sensor", "sensor"));
        }
        JdbcProbe.reset();
        send("hub", new DeviceRemovedEventAvro("sensor"));
        return JdbcProbe.executions.size();
    }

    private void assertNoScenarioRows() {
        for (String table : List.of("scenarios", "conditions", "actions", "scenario_conditions", "scenario_actions")) {
            assertEquals(0, count(table), table);
        }
    }

    private int count(String table) {
        Integer result = jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class);
        assertNotNull(result);
        return result;
    }

    private void add(String hub, String id) {
        send(hub, new DeviceAddedEventAvro(id, DeviceTypeAvro.SWITCH_SENSOR));
    }

    private void send(String hub, Object payload) {
        service.handle(new HubEventAvro(hub, Instant.parse("2024-08-06T15:11:24.157Z"), payload));
    }

    private ScenarioAddedEventAvro scenario(String name, String conditionSensor, String actionSensor) {
        return new ScenarioAddedEventAvro(name,
                List.of(new ScenarioConditionAvro(conditionSensor, ConditionTypeAvro.SWITCH,
                        ConditionOperationAvro.EQUALS, true)),
                List.of(new DeviceActionAvro(actionSensor, ActionTypeAvro.ACTIVATE, null)));
    }

    private ScenarioConditionAvro condition(ConditionTypeAvro type, Object value) {
        return new ScenarioConditionAvro("sensor", type, ConditionOperationAvro.EQUALS, value);
    }

    private DeviceActionAvro action(ActionTypeAvro type, Integer value) {
        return new DeviceActionAvro("sensor", type, value);
    }
}
