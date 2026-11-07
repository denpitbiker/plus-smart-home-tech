package ru.yandex.practicum.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import ru.yandex.practicum.inventory.entity.Inventory;
import ru.yandex.practicum.inventory.repository.InventoryRepository;
import ru.yandex.practicum.inventory.service.InventoryService;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

@SpringBootTest
@AutoConfigureMockMvc
class InventoryConcurrencyTest {
    private static final int CONCURRENT_REQUESTS = 2;
    private static final int BARRIER_TIMEOUT_SECONDS = 5;
    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    @Autowired private ObjectMapper json;
    @PersistenceContext private EntityManager entityManager;
    @Autowired private MockMvc mvc;
    @Autowired private InventoryService service;
    @SpyBean private InventoryRepository repository;

    @Test
    void onlyOneRequestCanReserveTheLastItem() throws Exception {
        long productId = Stubs.LAST_ITEM_PRODUCT_ID;
        service.create(Stubs.inventory(productId, Stubs.SINGLE_ITEM));
        synchronizeReads(productId);
        assertConflictingRequests(post("/api/inventory/reserve"), post("/api/inventory/reserve"),
                productId, Stubs.SINGLE_ITEM, Stubs.SINGLE_ITEM);
        var result = service.getByProductId(productId);
        assertThat(result.quantity()).isEqualTo(Stubs.SINGLE_ITEM);
        assertThat(result.reservedQuantity()).isEqualTo(Stubs.SINGLE_ITEM);
        assertThat(result.availableQuantity()).isZero();
    }

    @Test
    void quantityUpdateAndReservationCannotOverwriteEachOther() throws Exception {
        long productId = Stubs.UPDATE_AND_RESERVE_PRODUCT_ID;
        service.create(Stubs.inventory(productId, Stubs.SINGLE_ITEM));
        synchronizeReads(productId);
        assertConflictingRequests(put("/api/inventory"), post("/api/inventory/reserve"),
                productId, Stubs.EMPTY_STOCK, Stubs.SINGLE_ITEM);
        var result = service.getByProductId(productId);
        assertThat(result.availableQuantity()).isZero();
        assertThat(result.quantity()).isEqualTo(result.reservedQuantity());
        assertThat(result.quantity()).isBetween(Stubs.EMPTY_STOCK, Stubs.SINGLE_ITEM);
    }

    @Test
    void concurrentCreatesKeepOneRecord() throws Exception {
        long productId = Stubs.DUPLICATE_PRODUCT_ID;
        CyclicBarrier barrier = new CyclicBarrier(CONCURRENT_REQUESTS);
        doAnswer(invocation -> {
            Object result = !entityManager.createQuery("select i from Inventory i where i.productId = :productId")
                    .setParameter("productId", productId).getResultList().isEmpty();
            barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return result;
        }).when(repository).existsByProductId(productId);
        try (var executor = Executors.newFixedThreadPool(CONCURRENT_REQUESTS)) {
            var first = executor.submit(() -> send(post("/api/inventory"), productId, Stubs.SINGLE_ITEM));
            var second = executor.submit(() -> send(post("/api/inventory"), productId, Stubs.SINGLE_ITEM));
            assertThat(new int[]{first.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    second.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)})
                    .containsExactlyInAnyOrder(HttpStatus.CREATED.value(), HttpStatus.CONFLICT.value());
        }
        assertThat(service.getAll().stream().filter(item -> item.productId() == productId)).hasSize(Stubs.SINGLE_ITEM);
    }

    private void synchronizeReads(long productId) {
        CyclicBarrier barrier = new CyclicBarrier(CONCURRENT_REQUESTS);
        var reads = new AtomicInteger();
        doAnswer(invocation -> {
            Object result = entityManager.createQuery("select i from Inventory i where i.productId = :productId",
                            Inventory.class)
                    .setParameter("productId", productId).getResultStream().findFirst();
            // Both transactions must load the same version before either may update it.
            if (reads.incrementAndGet() <= CONCURRENT_REQUESTS) {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            return result;
        }).when(repository).findByProductId(productId);
    }

    private void assertConflictingRequests(MockHttpServletRequestBuilder firstRequest,
            MockHttpServletRequestBuilder secondRequest, long productId, int firstQuantity, int secondQuantity)
            throws Exception {
        try (var executor = Executors.newFixedThreadPool(CONCURRENT_REQUESTS)) {
            var first = executor.submit(() -> send(firstRequest, productId, firstQuantity));
            var second = executor.submit(() -> send(secondRequest, productId, secondQuantity));
            assertThat(new int[]{first.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    second.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)})
                    .containsExactlyInAnyOrder(HttpStatus.OK.value(), HttpStatus.CONFLICT.value());
        }
    }

    private int send(MockHttpServletRequestBuilder request, long productId, int quantity) throws Exception {
        return mvc.perform(request.contentType(MediaType.APPLICATION_JSON)
                .content(json.writeValueAsString(Stubs.inventory(productId, quantity))))
                .andReturn().getResponse().getStatus();
    }
}
