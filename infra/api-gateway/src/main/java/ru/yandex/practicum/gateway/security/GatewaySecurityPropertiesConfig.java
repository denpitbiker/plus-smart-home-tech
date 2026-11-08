package ru.yandex.practicum.gateway.security;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;

@Configuration(proxyBeanMethods = false)
public class GatewaySecurityPropertiesConfig {
    private static final String SECURITY_PREFIX = "app.security";

    @Bean
    public GatewaySecurityProperties gatewaySecurityProperties(ConfigurableEnvironment environment) {
        Binder binder = new Binder(ConfigurationPropertySources.get(environment),
                value -> value instanceof String text ? environment.resolveRequiredPlaceholders(text) : value);
        return binder.bind(SECURITY_PREFIX, GatewaySecurityProperties.class)
                .orElseThrow(() -> new IllegalArgumentException("app.security.users must not be empty"));
    }
}
