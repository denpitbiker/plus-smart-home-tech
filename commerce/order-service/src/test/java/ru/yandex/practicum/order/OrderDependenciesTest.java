package ru.yandex.practicum.order;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import ru.yandex.practicum.order.repository.OrderRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class OrderDependenciesTest {
    @Autowired private MockMvc mvc;
    @Autowired private Stubs stubs;
    @Autowired private CircuitBreakerRegistry breakers;
    @Autowired private OrderRepository orders;
    private long initialOrders;

    @BeforeEach
    void setup() {
        reset();
        initialOrders = orders.count();
    }

    @AfterEach
    void reset() {
        stubs.reset();
        breakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
    }

    @Test
    void callsBothServicesAndKeepsRequestSnapshot() throws Exception {
        create(Stubs.ORDER).andExpect(status().isCreated())
                .andExpect(jsonPath("$.items[0].productName").value("Snapshot lamp"))
                .andExpect(jsonPath("$.totalPrice").value(20.5));
        assertThat(stubs.requests).containsExactly("/api/products/42", "/api/inventory/42");
        assertThat(orders.count()).isEqualTo(initialOrders + 1);
    }

    @Test
    void rejectsInactiveProductsAndInsufficientCombinedQuantity() throws Exception {
        stubs.active = false;
        create(Stubs.ORDER).andExpect(status().isConflict());
        assertThat(stubs.requests).containsExactly("/api/products/42");
        stubs.reset();
        stubs.available = 3;
        String duplicate = Stubs.ORDER.replace("}]", "},"
                + "{\"productId\":42,\"productName\":\"Another\",\"quantity\":2,\"price\":10.25}]");
        create(duplicate).andExpect(status().isConflict());
        assertThat(stubs.requests).containsExactly("/api/products/42", "/api/inventory/42");
        assertThat(orders.count()).isEqualTo(initialOrders);
    }

    @Test
    void missingProductsDoNotOpenCircuit() throws Exception {
        stubs.productStatus = 404;
        for (int i = 0; i < 6; i++) {
            create(Stubs.ORDER).andExpect(status().isNotFound()).andExpect(jsonPath("$.status").value(404));
        }
        assertThat(stubs.requests).hasSize(6);
        assertThat(productBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(orders.count()).isEqualTo(initialOrders);
    }

    @Test
    void circuitOpensAfterFailuresAndRecoversAfterSuccessfulProbes() throws Exception {
        stubs.productStatus = 500;
        for (int i = 0; i < 5; i++) {
            create(Stubs.ORDER).andExpect(status().isServiceUnavailable());
        }
        assertThat(productBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);
        create(Stubs.ORDER).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503)).andExpect(jsonPath("$.timestamp").exists());
        assertThat(stubs.requests).hasSize(5);
        assertThat(orders.count()).isEqualTo(initialOrders);
        stubs.productStatus = 200;
        productBreaker().transitionToHalfOpenState();
        create(Stubs.ORDER).andExpect(status().isCreated());
        create(Stubs.ORDER).andExpect(status().isCreated());
        assertThat(productBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void inventoryFailureHasItsOwnCircuitAndNeverSavesOrder() throws Exception {
        stubs.inventoryStatus = 503;
        for (int i = 0; i < 6; i++) {
            create(Stubs.ORDER).andExpect(status().isServiceUnavailable());
        }
        assertThat(stubs.requests.stream().filter(path -> path.contains("inventory"))).hasSize(5);
        assertThat(productBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(breakers.circuitBreaker("InventoryClientgetInventoryLong").getState())
                .isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(orders.count()).isEqualTo(initialOrders);
    }

    @Test
    void missingInventoryDoesNotSaveOrder() throws Exception {
        stubs.inventoryStatus = 404;
        create(Stubs.ORDER).andExpect(status().isNotFound());
        assertThat(stubs.requests).containsExactly("/api/products/42", "/api/inventory/42");
        assertThat(orders.count()).isEqualTo(initialOrders);
    }

    @Test
    void timeoutReturnsServiceUnavailableWithoutSavingOrder() throws Exception {
        stubs.delayMillis = 1500;
        create(Stubs.ORDER).andExpect(status().isServiceUnavailable());
        assertThat(orders.count()).isEqualTo(initialOrders);
    }

    private CircuitBreaker productBreaker() {
        return breakers.circuitBreaker("ProductClientgetProductLong");
    }

    private ResultActions create(String body) throws Exception {
        return mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content(body));
    }
}
