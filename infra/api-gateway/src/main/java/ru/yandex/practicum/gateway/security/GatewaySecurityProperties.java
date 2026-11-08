package ru.yandex.practicum.gateway.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@ConfigurationProperties("app.security")
public record GatewaySecurityProperties(List<Account> users) {
    static final String USER_ROLE = "USER";
    static final String ADMIN_ROLE = "ADMIN";
    private static final Set<String> ALLOWED_ROLES = Set.of(USER_ROLE, ADMIN_ROLE);

    public GatewaySecurityProperties {
        if (users == null || users.isEmpty()) {
            throw new IllegalArgumentException("app.security.users must not be empty");
        }
        Set<String> usernames = new HashSet<>();
        for (Account user : users) {
            if (user == null || user.username() == null || user.username().isBlank()
                    || user.password() == null || user.password().isBlank()
                    || user.roles() == null || user.roles().isEmpty()
                    || !ALLOWED_ROLES.containsAll(user.roles())) {
                throw new IllegalArgumentException("Security user requires username, password and USER/ADMIN roles");
            }
            if (!usernames.add(user.username())) {
                throw new IllegalArgumentException("Duplicate security username: " + user.username());
            }
        }
        users = List.copyOf(users);
    }

    public record Account(String username, String password, List<String> roles) {
    }
}
