package ru.yandex.practicum.gateway.security;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public record GatewaySecurityProperties(List<Account> users) {
    private static final Pattern BCRYPT_HASH = Pattern.compile("\\$2[aby]\\$\\d{2}\\$[./A-Za-z0-9]{53}");

    public GatewaySecurityProperties {
        if (users == null || users.isEmpty()) {
            throw new IllegalArgumentException("app.security.users must not be empty");
        }
        Set<String> usernames = new HashSet<>();
        for (Account user : users) {
            if (user == null || user.username() == null || user.username().isBlank()
                    || user.roles() == null || user.roles().isEmpty()) {
                throw new IllegalArgumentException("Security user requires username, password and USER/ADMIN roles");
            }
            boolean hasPassword = user.password() != null && !user.password().isBlank();
            if (!hasPassword && (user.passwordHash() == null || !BCRYPT_HASH.matcher(user.passwordHash()).matches())) {
                throw new IllegalArgumentException("Security user requires a password or a BCrypt password hash");
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

    public record Account(String username, String password, String passwordHash, List<Role> roles) {
    }
}
