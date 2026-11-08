package ru.yandex.practicum.gateway;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import ru.yandex.practicum.gateway.Stubs.Request;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpStatus.GATEWAY_TIMEOUT;

@SpringBootTest(webEnvironment = RANDOM_PORT, properties = {
        "spring.config.location=classpath:gateway-config/api-gateway.yaml",
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false",
        "spring.cloud.gateway.httpclient.response-timeout=200ms"
})
class GatewayTimeoutTest {
    private static final String CATEGORIES_PATH = "/api/categories";
    private static final Duration BACKEND_DELAY = Duration.ofSeconds(2);
    private static final Stubs BACKEND = new Stubs("slow-catalog", BACKEND_DELAY);

    @Autowired
    private WebTestClient client;

    @DynamicPropertySource
    static void services(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.discovery.client.simple.instances.product-service[0].uri", BACKEND::uri);
    }

    @AfterAll
    static void stopBackend() {
        BACKEND.close();
    }

    @Test
    void returnsGatewayTimeoutWhenBackendDoesNotRespondInTime() {
        client.get().uri(CATEGORIES_PATH).exchange().expectStatus().isEqualTo(GATEWAY_TIMEOUT).expectBody().returnResult();
        assertThat(BACKEND.requests).containsExactly(new Request(GET, CATEGORIES_PATH));
    }
}
