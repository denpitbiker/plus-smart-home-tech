package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "sensors")
public class Sensor {
    @Id
    @Column(name = "id", nullable = false)
    private String id;

    @Column(name = "hub_id", nullable = false)
    private String hubId;

    protected Sensor() {
    }

    public Sensor(String id, String hubId) {
        this.id = id;
        this.hubId = hubId;
    }

    public String getId() {
        return id;
    }

    public String getHubId() {
        return hubId;
    }
}
