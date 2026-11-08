package ru.yandex.practicum.gateway.security;

import org.springframework.cloud.gateway.config.GlobalCorsProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.context.NoOpServerSecurityContextRepository;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.security.config.Customizer.withDefaults;
import static ru.yandex.practicum.gateway.security.Role.ADMIN;
import static ru.yandex.practicum.gateway.security.Role.USER;

@Configuration
@EnableWebFluxSecurity
public class GatewaySecurityConfig {
    private static final String HEALTH_PATH = "/actuator/health";
    private static final String ALL_PATHS = "/**";
    private static final String[] DOCUMENTATION_PATHS = {
            "/product-service/v3/api-docs", "/product-service/v3/api-docs/**",
            "/product-service/swagger-ui.html", "/product-service/swagger-ui/**",
            "/inventory-service/v3/api-docs", "/inventory-service/v3/api-docs/**",
            "/inventory-service/swagger-ui.html", "/inventory-service/swagger-ui/**",
            "/order-service/v3/api-docs", "/order-service/v3/api-docs/**",
            "/order-service/swagger-ui.html", "/order-service/swagger-ui/**"
    };
    private static final String[] ORDER_LIST_PATHS = {"/api/orders", "/api/orders/"};
    private static final String[] ORDER_READ_PATHS = {"/api/orders/by-email", "/api/orders/{id:[0-9]+}"};
    private static final String[] ORDER_CREATE_PATHS = {"/api/orders", "/api/orders/**"};
    private static final String[] CATALOG_PATHS = {
            "/api/products", "/api/products/**", "/api/categories", "/api/categories/**",
            "/api/inventory", "/api/inventory/**"
    };

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http,
                                                       CorsConfigurationSource corsConfigurationSource) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .cors(cors -> cors.configurationSource(corsConfigurationSource))
                .httpBasic(withDefaults())
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .logout(ServerHttpSecurity.LogoutSpec::disable)
                .requestCache(ServerHttpSecurity.RequestCacheSpec::disable)
                .securityContextRepository(NoOpServerSecurityContextRepository.getInstance())
                .authorizeExchange(access -> access
                        .pathMatchers(HttpMethod.OPTIONS, ALL_PATHS).permitAll()
                        .pathMatchers(HttpMethod.GET, HEALTH_PATH).permitAll()
                        .pathMatchers(HttpMethod.GET, DOCUMENTATION_PATHS).permitAll()
                        .pathMatchers(HttpMethod.GET, CATALOG_PATHS).permitAll()
                        .pathMatchers(HttpMethod.GET, ORDER_LIST_PATHS).hasRole(ADMIN.name())
                        .pathMatchers(HttpMethod.GET, ORDER_READ_PATHS).hasRole(USER.name())
                        .pathMatchers(HttpMethod.POST, ORDER_CREATE_PATHS).hasRole(USER.name())
                        .pathMatchers(HttpMethod.POST, CATALOG_PATHS).hasRole(ADMIN.name())
                        .pathMatchers(HttpMethod.PUT, CATALOG_PATHS).hasRole(ADMIN.name())
                        .pathMatchers(HttpMethod.PATCH, CATALOG_PATHS).hasRole(ADMIN.name())
                        .pathMatchers(HttpMethod.DELETE, CATALOG_PATHS).hasRole(ADMIN.name())
                        .anyExchange().denyAll())
                .build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public MapReactiveUserDetailsService userDetailsService(GatewaySecurityProperties properties,
                                                           PasswordEncoder encoder) {
        List<UserDetails> users = new ArrayList<>();
        for (GatewaySecurityProperties.Account account : properties.users()) {
            String[] roles = new String[account.roles().size()];
            for (int i = 0; i < roles.length; i++) {
                roles[i] = account.roles().get(i).name();
            }
            String password = account.password() != null && !account.password().isBlank()
                    ? encoder.encode(account.password()) : account.passwordHash();
            UserDetails user = User.withUsername(account.username())
                    .password(password)
                    .roles(roles)
                    .build();
            users.add(user);
        }
        return new MapReactiveUserDetailsService(users);
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource(GlobalCorsProperties properties) {
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.setCorsConfigurations(properties.getCorsConfigurations());
        return source;
    }
}
