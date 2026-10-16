CREATE TABLE scenarios (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hub_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    UNIQUE (hub_id, name)
);

CREATE TABLE sensors (
    id VARCHAR PRIMARY KEY,
    hub_id VARCHAR NOT NULL
);

CREATE TABLE conditions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type VARCHAR NOT NULL,
    operation VARCHAR NOT NULL,
    value INTEGER
);

CREATE TABLE actions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type VARCHAR NOT NULL,
    value INTEGER
);

CREATE TABLE scenario_conditions (
    scenario_id BIGINT NOT NULL REFERENCES scenarios(id),
    sensor_id VARCHAR NOT NULL REFERENCES sensors(id),
    condition_id BIGINT NOT NULL REFERENCES conditions(id),
    PRIMARY KEY (scenario_id, sensor_id, condition_id)
);

CREATE TABLE scenario_actions (
    scenario_id BIGINT NOT NULL REFERENCES scenarios(id),
    sensor_id VARCHAR NOT NULL REFERENCES sensors(id),
    action_id BIGINT NOT NULL REFERENCES actions(id),
    PRIMARY KEY (scenario_id, sensor_id, action_id)
);

CREATE INDEX scenarios_hub_id_idx ON scenarios(hub_id);
CREATE INDEX scenario_conditions_sensor_id_idx ON scenario_conditions(sensor_id);
CREATE INDEX scenario_actions_sensor_id_idx ON scenario_actions(sensor_id);

CREATE FUNCTION check_hub_id()
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT hub_id FROM scenarios WHERE id = NEW.scenario_id)
       IS DISTINCT FROM
       (SELECT hub_id FROM sensors WHERE id = NEW.sensor_id) THEN
        RAISE EXCEPTION 'Hub IDs do not match for scenario_id % and sensor_id %',
            NEW.scenario_id, NEW.sensor_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_bi_scenario_conditions_hub_id_check
BEFORE INSERT OR UPDATE ON scenario_conditions
FOR EACH ROW EXECUTE FUNCTION check_hub_id();

CREATE TRIGGER tr_bi_scenario_actions_hub_id_check
BEFORE INSERT OR UPDATE ON scenario_actions
FOR EACH ROW EXECUTE FUNCTION check_hub_id();
