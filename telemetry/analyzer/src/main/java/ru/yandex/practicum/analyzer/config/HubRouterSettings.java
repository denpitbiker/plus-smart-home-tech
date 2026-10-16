package ru.yandex.practicum.analyzer.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("analyzer.hub-router")
public record HubRouterSettings(Duration deadline) {

    public HubRouterSettings {
        if (deadline == null || deadline.isNegative() || deadline.isZero()) {
            throw new IllegalArgumentException("Hub Router deadline must be positive");
        }
    }
}
