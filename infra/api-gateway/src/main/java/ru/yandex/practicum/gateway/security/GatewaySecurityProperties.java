package ru.yandex.practicum.gateway.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@ConfigurationProperties("app.security")
public record GatewaySecurityProperties(List<Account> users) {
    public GatewaySecurityProperties {
        if (users == null || users.isEmpty()) {
            throw new IllegalArgumentException("app.security.users must not be empty");
        }
        Set<String> usernames = new HashSet<>();
        for (Account user : users) {
            if (user == null || user.username() == null || user.username().isBlank()
                    || user.password() == null || user.password().isBlank()
                    || user.roles() == null || user.roles().isEmpty()) {
                throw new IllegalArgumentException("Security user requires username, password and USER/ADMIN roles");
            }
            if (user.password().startsWith("${") && user.password().endsWith("}")) {
                throw new IllegalArgumentException("Security password environment variable is not set");
            }
            for (Role role : user.roles()) {
                if (role == null) {
                    throw new IllegalArgumentException("Security role must not be null");
                }
            }
            if (!usernames.add(user.username())) {
                throw new IllegalArgumentException("Duplicate security username: " + user.username());
            }
        }
        users = List.copyOf(users);
    }

    public record Account(String username, String password, List<Role> roles) {
    }
}
