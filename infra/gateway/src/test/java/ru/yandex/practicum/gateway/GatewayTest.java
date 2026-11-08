package ru.yandex.practicum.gateway;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT, properties = {
        "spring.config.location=classpath:gateway-config/application.yaml",
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false"
})
class GatewayTest {
    private static final Stubs PRODUCTS = new Stubs("product-service");
    private static final Stubs INVENTORY = new Stubs("inventory-service");
    private static final Stubs ORDERS = new Stubs("order-service");

    @Autowired
    private WebTestClient client;

    @DynamicPropertySource
    static void services(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.discovery.client.simple.instances.product-service[0].uri", PRODUCTS::uri);
        registry.add("spring.cloud.discovery.client.simple.instances.inventory-service[0].uri", INVENTORY::uri);
        registry.add("spring.cloud.discovery.client.simple.instances.order-service[0].uri", ORDERS::uri);
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
    void routesRequestsAndPreservesQueryParameters() {
        for (String path : List.of("/products", "/products/search?query=lamp", "/categories", "/categories/1")) {
            client.get().uri(path).exchange().expectStatus().isOk()
                    .expectBody().jsonPath("$.service").isEqualTo("product-service");
            assertThat(PRODUCTS.requests).contains("GET /api" + path);
        }
        client.get().uri("/inventory/1").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.service").isEqualTo("inventory-service");
        assertThat(INVENTORY.requests).containsExactly("GET /api/inventory/1");

        client.get().uri("/orders/by-email?email=user@example.com").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.service").isEqualTo("order-service");
        assertThat(ORDERS.requests).containsExactly("GET /api/orders/by-email?email=user@example.com");
    }

    @Test
    void retriesProductNotFoundUntilSuccess() {
        PRODUCTS.statuses.addAll(List.of(404, 404, 200));
        client.get().uri("/products/1").exchange().expectStatus().isOk();
        assertThat(PRODUCTS.requests).containsExactly(
                "GET /api/products/1", "GET /api/products/1", "GET /api/products/1");
    }

    @Test
    void returnsLastErrorAfterThreeRetries() {
        PRODUCTS.statuses.addAll(List.of(404, 404, 404, 404));
        client.get().uri("/products/1").exchange().expectStatus().isNotFound()
                .expectBody().json(Stubs.ERROR);
        assertThat(PRODUCTS.requests).hasSize(4).containsOnly("GET /api/products/1");
    }

    @Test
    void doesNotRetryOtherStatusesOrCategoryErrors() {
        PRODUCTS.statuses.add(500);
        client.get().uri("/products/1").exchange().expectStatus().is5xxServerError();
        PRODUCTS.statuses.add(404);
        client.get().uri("/categories/1").exchange().expectStatus().isNotFound();
        assertThat(PRODUCTS.requests).containsExactly("GET /api/products/1", "GET /api/categories/1");
    }

    @Test
    void forwardsWriteBodyWithoutRetrying() {
        String body = "{\"name\":\"Lamp\",\"price\":100,\"categoryId\":1}";
        PRODUCTS.statuses.addAll(List.of(404, 404));
        client.post().uri("/products").contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isNotFound();
        client.patch().uri("/products/1").contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isNotFound();
        assertThat(PRODUCTS.requests).containsExactly("POST /api/products", "PATCH /api/products/1");
        assertThat(PRODUCTS.bodies).containsExactly(body, body);
    }
}
