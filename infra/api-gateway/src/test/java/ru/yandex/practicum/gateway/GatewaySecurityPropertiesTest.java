package ru.yandex.practicum.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.gateway.security.GatewaySecurityConfig;
import ru.yandex.practicum.gateway.security.GatewaySecurityProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.yandex.practicum.gateway.GatewayTestData.IVAN_PASSWORD;
import static ru.yandex.practicum.gateway.GatewayTestData.IVAN_USERNAME;
import static ru.yandex.practicum.gateway.GatewayTestData.WRONG_PASSWORD;

class GatewaySecurityPropertiesTest {
    private static final String USERNAME_PROPERTY = "app.security.users[0].username=" + IVAN_USERNAME;
    private static final String PASSWORD_PROPERTY = "app.security.users[0].password=" + IVAN_PASSWORD;
    private static final String MISSING_PASSWORD_PROPERTY = "app.security.users[0].password=${MISSING_TEST_PASSWORD}";
    private static final String UNKNOWN_ROLE_PROPERTY = "app.security.users[0].roles[0]=MANAGER";
    private static final String USER_ROLE_PROPERTY = "app.security.users[0].roles[0]=USER";
    private static final String[] VALID_USER_PROPERTIES = {
            USERNAME_PROPERTY, PASSWORD_PROPERTY, USER_ROLE_PROPERTY
    };
    private static final String[] DUPLICATE_USER_PROPERTIES = {
            USERNAME_PROPERTY, PASSWORD_PROPERTY, USER_ROLE_PROPERTY,
            "app.security.users[1].username=" + IVAN_USERNAME,
            "app.security.users[1].password=other", "app.security.users[1].roles[0]=ADMIN"
    };

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(PropertiesConfig.class);

    @Test
    void bindsUsersAndEncodesPasswords() {
        context.withPropertyValues(VALID_USER_PROPERTIES)
                .run(app -> {
                    assertThat(app).hasNotFailed();
                    var config = new GatewaySecurityConfig();
                    var encoder = config.passwordEncoder();
                    var users = config.userDetailsService(app.getBean(GatewaySecurityProperties.class), encoder);
                    var ivan = users.findByUsername(IVAN_USERNAME).block();
                    assertThat(ivan).isNotNull();
                    assertThat(ivan.getPassword()).isNotEqualTo(IVAN_PASSWORD);
                    assertThat(encoder.matches(IVAN_PASSWORD, ivan.getPassword())).isTrue();
                    assertThat(encoder.matches(WRONG_PASSWORD, ivan.getPassword())).isFalse();
                });
    }

    @Test
    void rejectsMissingUsersAndIncompleteAccounts() {
        context.run(app -> assertThat(app).hasFailed());
        context.withPropertyValues(USERNAME_PROPERTY)
                .run(app -> assertThat(app).hasFailed());
    }

    @Test
    void rejectsDuplicateUsernames() {
        context.withPropertyValues(DUPLICATE_USER_PROPERTIES)
                .run(app -> assertThat(app).hasFailed());
    }

    @Test
    void rejectsMissingPasswordEnvironmentVariable() {
        context.withPropertyValues(USERNAME_PROPERTY, MISSING_PASSWORD_PROPERTY, USER_ROLE_PROPERTY)
                .run(app -> assertThat(app).hasFailed());
    }

    @Test
    void rejectsUnknownRoles() {
        context.withPropertyValues(USERNAME_PROPERTY, PASSWORD_PROPERTY, UNKNOWN_ROLE_PROPERTY)
                .run(app -> assertThat(app).hasFailed());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(GatewaySecurityProperties.class)
    static class PropertiesConfig {
    }
}
