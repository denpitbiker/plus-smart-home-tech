package ru.yandex.practicum.gateway;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import ru.yandex.practicum.gateway.Stubs.Request;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.OPTIONS;
import static org.springframework.http.HttpMethod.POST;
import static ru.yandex.practicum.gateway.GatewayTestData.ANNA_PASSWORD;
import static ru.yandex.practicum.gateway.GatewayTestData.ANNA_USERNAME;
import static ru.yandex.practicum.gateway.GatewayTestData.IVAN_PASSWORD;
import static ru.yandex.practicum.gateway.GatewayTestData.IVAN_USERNAME;
import static ru.yandex.practicum.gateway.GatewayTestData.WRONG_PASSWORD;

@SpringBootTest(webEnvironment = RANDOM_PORT, properties = {
        "spring.config.location=classpath:gateway-config/application.yaml",
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false"
})
class GatewaySecurityConfigTest {
    private static final Stubs BACKEND = new Stubs("test-backend");
    private static final String UI_ORIGIN = "http://localhost:8443";
    private static final String PRODUCTS_PATH = "/api/products";
    private static final String PRODUCT_PATH = "/api/products/1";
    private static final String CATEGORIES_PATH = "/api/categories";
    private static final String INVENTORY_PATH = "/api/inventory";
    private static final String ORDER_PATH = "/api/orders/1";
    private static final String ORDERS_PATH = "/api/orders";
    private static final String SERVICE_DOCUMENTATION_PATH = "/%s%s";
    private static final String UNTRUSTED_ORIGIN = "http://untrusted.example";
    private static final String PREFLIGHT_HEADERS = "Authorization,Content-Type";
    private static final List<Credentials> USER_CREDENTIALS = List.of(
            new Credentials(IVAN_USERNAME, IVAN_PASSWORD), new Credentials(ANNA_USERNAME, ANNA_PASSWORD));
    private static final List<String> SERVICE_NAMES = List.of(
            "product-service", "inventory-service", "order-service");
    private static final List<String> PUBLIC_CATALOG_PATHS = List.of(
            PRODUCTS_PATH, PRODUCT_PATH, CATEGORIES_PATH, INVENTORY_PATH);
    private static final List<String> USER_ORDER_PATHS = List.of(
            ORDER_PATH, "/api/orders/by-email?email=ivan@example.com");
    private static final List<String> ORDER_LIST_PATHS = List.of(
            ORDERS_PATH, "/api/orders/", "/api/orders?email=ivan@example.com");
    private static final List<HttpMethod> WRITE_METHODS = List.of(
            HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.DELETE);
    private static final List<String> CATALOG_WRITE_PATHS = List.of(
            PRODUCT_PATH, CATEGORIES_PATH, INVENTORY_PATH);
    private static final List<String> FORBIDDEN_PATHS = List.of(
            "/unknown", "/products", "/orders", "/api/orders/unknown",
            "/api/orders/1/details", "/product-service/api/products", "/PRODUCT-SERVICE/api/products",
            "/product-service/actuator", "/login", "/logout");
    private static final List<String> DOCUMENTATION_PATHS = List.of(
            "/v3/api-docs", "/v3/api-docs/swagger-config",
            "/swagger-ui.html", "/swagger-ui/index.html");

    @Autowired
    private WebTestClient client;

    @DynamicPropertySource
    static void services(DynamicPropertyRegistry registry) {
        for (String service : SERVICE_NAMES) {
            registry.add("spring.cloud.discovery.client.simple.instances." + service + "[0].uri", BACKEND::uri);
        }
    }

    @BeforeEach
    void reset() {
        BACKEND.reset();
    }

    @AfterAll
    static void stopBackend() {
        BACKEND.close();
    }

    @Test
    void catalogReadsArePublic() {
        for (String path : PUBLIC_CATALOG_PATHS) {
            client.get().uri(path).exchange().expectStatus().isOk();
            assertThat(BACKEND.requests).contains(new Request(GET, path));
        }
    }

    @Test
    void ordersRequireCredentialsAndRejectWrongPassword() {
        client.post().uri(ORDERS_PATH).exchange().expectStatus().isUnauthorized()
                .expectHeader().valueMatches(HttpHeaders.WWW_AUTHENTICATE, "Basic.*");
        client.post().uri(ORDERS_PATH).headers(h -> h.setBasicAuth(IVAN_USERNAME, WRONG_PASSWORD))
                .exchange().expectStatus().isUnauthorized();
        assertThat(BACKEND.requests).isEmpty();
    }

    @Test
    void usersCanCreateAndReadIndividualOrders() {
        for (Credentials credentials : USER_CREDENTIALS) {
            client.post().uri(ORDERS_PATH).headers(h -> h.setBasicAuth(credentials.username(), credentials.password()))
                    .exchange().expectStatus().isOk();
            for (String path : USER_ORDER_PATHS) {
                client.get().uri(path).headers(h -> h.setBasicAuth(credentials.username(), credentials.password()))
                        .exchange().expectStatus().isOk();
            }
        }
        assertThat(BACKEND.requests).hasSize(USER_CREDENTIALS.size() * (1 + USER_ORDER_PATHS.size()));
    }

    @Test
    void onlyAdminCanListOrdersIncludingTrailingSlash() {
        for (String path : ORDER_LIST_PATHS) {
            client.get().uri(path).headers(h -> h.setBasicAuth(IVAN_USERNAME, IVAN_PASSWORD))
                    .exchange().expectStatus().isForbidden();
        }
        assertThat(BACKEND.requests).isEmpty();
        client.get().uri(ORDERS_PATH).headers(h -> h.setBasicAuth(ANNA_USERNAME, ANNA_PASSWORD))
                .exchange().expectStatus().isOk();
        assertThat(BACKEND.requests).containsExactly(new Request(GET, ORDERS_PATH));
    }

    @Test
    void catalogWritesRequireAdmin() {
        for (HttpMethod method : WRITE_METHODS) {
            for (String path : CATALOG_WRITE_PATHS) {
                client.method(method).uri(path).headers(h -> h.setBasicAuth(IVAN_USERNAME, IVAN_PASSWORD))
                        .exchange().expectStatus().isForbidden();
            }
        }
        assertThat(BACKEND.requests).isEmpty();
        for (HttpMethod method : WRITE_METHODS) {
            for (String path : CATALOG_WRITE_PATHS) {
                client.method(method).uri(path).headers(h -> h.setBasicAuth(ANNA_USERNAME, ANNA_PASSWORD))
                        .exchange().expectStatus().isOk();
            }
        }
        assertThat(BACKEND.requests).hasSize(WRITE_METHODS.size() * CATALOG_WRITE_PATHS.size());
    }

    @Test
    void unknownRoutesAndMethodsAreDeniedEvenForAdmin() {
        for (String path : FORBIDDEN_PATHS) {
            client.get().uri(path).headers(h -> h.setBasicAuth(ANNA_USERNAME, ANNA_PASSWORD))
                    .exchange().expectStatus().isForbidden();
        }
        client.delete().uri(ORDER_PATH).headers(h -> h.setBasicAuth(ANNA_USERNAME, ANNA_PASSWORD))
                .exchange().expectStatus().isForbidden();
        client.head().uri(PRODUCTS_PATH).headers(h -> h.setBasicAuth(ANNA_USERNAME, ANNA_PASSWORD))
                .exchange().expectStatus().isForbidden();
        assertThat(BACKEND.requests).isEmpty();
    }

    @Test
    void credentialsAreNotSavedInSession() {
        client.post().uri(ORDERS_PATH).headers(h -> h.setBasicAuth(IVAN_USERNAME, IVAN_PASSWORD))
                .exchange().expectStatus().isOk().expectHeader().doesNotExist(HttpHeaders.SET_COOKIE);
        client.post().uri(ORDERS_PATH).exchange().expectStatus().isUnauthorized()
                .expectHeader().doesNotExist(HttpHeaders.SET_COOKIE);
        assertThat(BACKEND.requests).containsExactly(new Request(POST, ORDERS_PATH));
    }

    @Test
    void optionsAndCorsPreflightArePublic() {
        client.options().uri(ORDERS_PATH).exchange().expectStatus().isOk();
        assertThat(BACKEND.requests).containsExactly(new Request(OPTIONS, ORDERS_PATH));
        BACKEND.reset();
        client.options().uri(ORDERS_PATH)
                .header(HttpHeaders.ORIGIN, UI_ORIGIN)
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, POST.name())
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS, PREFLIGHT_HEADERS)
                .exchange().expectStatus().isOk()
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, UI_ORIGIN)
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true")
                .expectHeader().valueMatches(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, "(?i).*authorization.*");
        assertThat(BACKEND.requests).isEmpty();
    }

    @Test
    void corsRejectsUnknownOriginAndAddsHeadersToSecurityErrors() {
        client.options().uri(ORDERS_PATH).header(HttpHeaders.ORIGIN, UNTRUSTED_ORIGIN)
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, POST.name())
                .exchange().expectStatus().isForbidden();
        client.post().uri(ORDERS_PATH).header(HttpHeaders.ORIGIN, UI_ORIGIN)
                .exchange().expectStatus().isUnauthorized()
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, UI_ORIGIN);
        assertThat(BACKEND.requests).isEmpty();
    }

    @Test
    void documentationIsPublicAndStripsOnlyServicePrefix() {
        for (String service : SERVICE_NAMES) {
            for (String path : DOCUMENTATION_PATHS) {
                client.get().uri(SERVICE_DOCUMENTATION_PATH.formatted(service, path)).exchange().expectStatus().isOk();
                assertThat(BACKEND.requests).contains(new Request(GET, path));
            }
        }
        assertThat(BACKEND.requests).hasSize(SERVICE_NAMES.size() * DOCUMENTATION_PATHS.size());
    }

    private record Credentials(String username, String password) {
    }

}
