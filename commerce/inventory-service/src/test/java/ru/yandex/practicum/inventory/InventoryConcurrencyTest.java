package ru.yandex.practicum.inventory;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.repository.InventoryRepository;
import ru.yandex.practicum.inventory.service.InventoryService;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

@SpringBootTest
@AutoConfigureMockMvc
class InventoryConcurrencyTest {
    @jakarta.persistence.PersistenceContext private jakarta.persistence.EntityManager entityManager;
    @Autowired private MockMvc mvc;
    @Autowired private InventoryService service;
    @SpyBean private InventoryRepository repository;

    @Test
    void onlyOneRequestCanReserveTheLastItem() throws Exception {
        long productId = 300001;
        service.create(new UpdateInventoryRequest(productId, 1));
        synchronizeReads(productId);
        assertConflictingRequests(post("/api/inventory/reserve"), post("/api/inventory/reserve"),
                productId, 1, 1);
        var result = service.getByProductId(productId);
        assertThat(result.quantity()).isEqualTo(1);
        assertThat(result.reservedQuantity()).isEqualTo(1);
        assertThat(result.availableQuantity()).isZero();
    }

    @Test
    void quantityUpdateAndReservationCannotOverwriteEachOther() throws Exception {
        long productId = 300002;
        service.create(new UpdateInventoryRequest(productId, 1));
        synchronizeReads(productId);
        assertConflictingRequests(put("/api/inventory"), post("/api/inventory/reserve"), productId, 0, 1);
        var result = service.getByProductId(productId);
        assertThat(result.availableQuantity()).isZero();
        assertThat(result.quantity()).isEqualTo(result.reservedQuantity());
        assertThat(result.quantity()).isBetween(0, 1);
    }

    @Test
    void concurrentCreatesKeepOneRecord() throws Exception {
        long productId = 300003;
        CyclicBarrier barrier = new CyclicBarrier(2);
        doAnswer(invocation -> {
            Object result = !entityManager.createQuery("select i from Inventory i where i.productId = :productId")
                    .setParameter("productId", productId).getResultList().isEmpty();
            barrier.await(5, TimeUnit.SECONDS);
            return result;
        }).when(repository).existsByProductId(productId);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> send(post("/api/inventory"), productId, 1));
            var second = executor.submit(() -> send(post("/api/inventory"), productId, 1));
            assertThat(new int[]{first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)})
                    .containsExactlyInAnyOrder(201, 409);
        }
        assertThat(service.getAll().stream().filter(item -> item.productId() == productId)).hasSize(1);
    }

    private void synchronizeReads(long productId) {
        CyclicBarrier barrier = new CyclicBarrier(2);
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        doAnswer(invocation -> {
            Object result = entityManager.createQuery("select i from Inventory i where i.productId = :productId",
                            ru.yandex.practicum.inventory.entity.Inventory.class)
                    .setParameter("productId", productId).getResultStream().findFirst();
            // Both transactions must load the same version before either may update it.
            if (reads.incrementAndGet() <= 2) barrier.await(5, TimeUnit.SECONDS);
            return result;
        }).when(repository).findByProductId(productId);
    }

    private void assertConflictingRequests(MockHttpServletRequestBuilder firstRequest,
            MockHttpServletRequestBuilder secondRequest, long productId, int firstQuantity, int secondQuantity)
            throws Exception {
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> send(firstRequest, productId, firstQuantity));
            var second = executor.submit(() -> send(secondRequest, productId, secondQuantity));
            assertThat(new int[]{first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)})
                    .containsExactlyInAnyOrder(200, 409);
        }
    }

    private int send(MockHttpServletRequestBuilder request, long productId, int quantity) throws Exception {
        return mvc.perform(request.contentType(MediaType.APPLICATION_JSON)
                .content("{\"productId\":" + productId + ",\"quantity\":" + quantity + "}"))
                .andReturn().getResponse().getStatus();
    }
}
