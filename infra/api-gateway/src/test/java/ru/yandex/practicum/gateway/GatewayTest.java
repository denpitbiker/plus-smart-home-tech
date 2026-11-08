package ru.yandex.practicum.gateway;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import ru.yandex.practicum.gateway.Stubs.Request;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.PATCH;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.OK;
import static ru.yandex.practicum.gateway.GatewayTestData.ANNA_PASSWORD;
import static ru.yandex.practicum.gateway.GatewayTestData.ANNA_USERNAME;
import static ru.yandex.practicum.gateway.GatewayTestData.IVAN_PASSWORD;
import static ru.yandex.practicum.gateway.GatewayTestData.IVAN_USERNAME;

@SpringBootTest(webEnvironment = RANDOM_PORT, properties = {
        "spring.config.location=classpath:gateway-config/api-gateway.yaml",
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false"
})
class GatewayTest {
    private static final String PRODUCT_SERVICE = "product-service";
    private static final String INVENTORY_SERVICE = "inventory-service";
    private static final String ORDER_SERVICE = "order-service";
    private static final String PRODUCTS_PATH = "/api/products";
    private static final String PRODUCT_PATH = "/api/products/1";
    private static final String PRODUCT_SEARCH_PATH = "/api/products/search?query=lamp";
    private static final String CATEGORIES_PATH = "/api/categories";
    private static final String CATEGORY_PATH = "/api/categories/1";
    private static final String INVENTORY_PATH = "/api/inventory/1";
    private static final String ORDERS_BY_EMAIL_PATH = "/api/orders/by-email?email=user@example.com";
    private static final String SERVICE_JSON_PATH = "$.service";

    private static final List<String> CATALOG_PATHS = List.of(
            PRODUCTS_PATH, PRODUCT_SEARCH_PATH, CATEGORIES_PATH, CATEGORY_PATH);
    private static final List<Integer> RECOVERING_RESPONSES = List.of(NOT_FOUND.value(), NOT_FOUND.value(), OK.value());
    private static final List<Integer> NOT_FOUND_RESPONSES = List.of(
            NOT_FOUND.value(), NOT_FOUND.value(), NOT_FOUND.value(), NOT_FOUND.value());
    private static final List<Integer> NON_RETRYABLE_RESPONSES = List.of(INTERNAL_SERVER_ERROR.value(), NOT_FOUND.value());
    private static final List<Integer> WRITE_RESPONSES = List.of(NOT_FOUND.value(), NOT_FOUND.value());
    private static final String PRODUCT_BODY = """
            {"name":"Lamp","price":100,"categoryId":1}
            """;

    private static final Stubs PRODUCTS = new Stubs(PRODUCT_SERVICE);
    private static final Stubs INVENTORY = new Stubs(INVENTORY_SERVICE);
    private static final Stubs ORDERS = new Stubs(ORDER_SERVICE);

    @Autowired
    private WebTestClient client;

    @DynamicPropertySource
    static void services(DynamicPropertyRegistry registry) {
        registry.add(discoveryProperty(PRODUCT_SERVICE), PRODUCTS::uri);
        registry.add(discoveryProperty(INVENTORY_SERVICE), INVENTORY::uri);
        registry.add(discoveryProperty(ORDER_SERVICE), ORDERS::uri);
    }

    private static String discoveryProperty(String service) {
        return "spring.cloud.discovery.client.simple.instances." + service + "[0].uri";
    }

    @BeforeEach
    void reset() {
        PRODUCTS.reset();
        INVENTORY.reset();
        ORDERS.reset();
    }

    @AfterAll
    static void stopServices() {
        PRODUCTS.close();
        INVENTORY.close();
        ORDERS.close();
    }

    @Test
    @DisplayName("Маршруты сохраняют параметры запроса и выбирают нужный сервис")
    void routesRequestsAndPreservesQueryParameters() {
        // Given: сервисы зарегистрированы в services(), заглушки сброшены в reset().
        // When / Then
        for (String path : CATALOG_PATHS) {
            client.get().uri(path).exchange().expectStatus().isOk()
                    .expectBody().jsonPath(SERVICE_JSON_PATH).isEqualTo(PRODUCT_SERVICE);
            assertThat(PRODUCTS.requests).contains(new Request(GET, path));
        }
        client.get().uri(INVENTORY_PATH).exchange().expectStatus().isOk()
                .expectBody().jsonPath(SERVICE_JSON_PATH).isEqualTo(INVENTORY_SERVICE);
        assertThat(INVENTORY.requests).containsExactly(new Request(GET, INVENTORY_PATH));

        client.get().uri(ORDERS_BY_EMAIL_PATH)
                .headers(headers -> headers.setBasicAuth(IVAN_USERNAME, IVAN_PASSWORD))
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath(SERVICE_JSON_PATH).isEqualTo(ORDER_SERVICE);
        assertThat(ORDERS.requests).containsExactly(new Request(GET, ORDERS_BY_EMAIL_PATH));
    }

    @Test
    @DisplayName("GET товара повторяется после 404 до успешного ответа")
    void retriesProductNotFoundUntilSuccess() {
        // Given
        PRODUCTS.statuses.addAll(RECOVERING_RESPONSES);
        // When / Then
        client.get().uri(PRODUCT_PATH).exchange().expectStatus().isOk().expectBody().returnResult();
        assertThat(PRODUCTS.requests).hasSize(RECOVERING_RESPONSES.size())
                .containsOnly(new Request(GET, PRODUCT_PATH));
    }

    @Test
    @DisplayName("После трёх повторов возвращается последняя ошибка")
    void returnsLastErrorAfterThreeRetries() {
        // Given
        PRODUCTS.statuses.addAll(NOT_FOUND_RESPONSES);
        // When / Then
        client.get().uri(PRODUCT_PATH).exchange().expectStatus().isNotFound()
                .expectBody().json(Stubs.ERROR);
        assertThat(PRODUCTS.requests).hasSize(NOT_FOUND_RESPONSES.size()).containsOnly(new Request(GET, PRODUCT_PATH));
    }

    @Test
    @DisplayName("Ошибки 500 и ошибки категорий не повторяются")
    void doesNotRetryOtherStatusesOrCategoryErrors() {
        // Given
        PRODUCTS.statuses.addAll(NON_RETRYABLE_RESPONSES);
        // When / Then
        client.get().uri(PRODUCT_PATH).exchange().expectStatus().is5xxServerError().expectBody().returnResult();
        client.get().uri(CATEGORY_PATH).exchange().expectStatus().isNotFound().expectBody().returnResult();
        assertThat(PRODUCTS.requests).containsExactly(new Request(GET, PRODUCT_PATH), new Request(GET, CATEGORY_PATH));
    }

    @Test
    @DisplayName("Запись передаёт тело запроса без повторов")
    void forwardsWriteBodyWithoutRetrying() {
        // Given
        PRODUCTS.statuses.addAll(WRITE_RESPONSES);
        // When / Then
        client.post().uri(PRODUCTS_PATH)
                .headers(headers -> headers.setBasicAuth(ANNA_USERNAME, ANNA_PASSWORD))
                .contentType(MediaType.APPLICATION_JSON).bodyValue(PRODUCT_BODY)
                .exchange().expectStatus().isNotFound().expectBody().returnResult();
        client.patch().uri(PRODUCT_PATH)
                .headers(headers -> headers.setBasicAuth(ANNA_USERNAME, ANNA_PASSWORD))
                .contentType(MediaType.APPLICATION_JSON).bodyValue(PRODUCT_BODY)
                .exchange().expectStatus().isNotFound().expectBody().returnResult();
        assertThat(PRODUCTS.requests).containsExactly(new Request(POST, PRODUCTS_PATH), new Request(PATCH, PRODUCT_PATH));
        assertThat(PRODUCTS.bodies).containsExactly(PRODUCT_BODY, PRODUCT_BODY);
    }
}
