package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.client.circuitbreaker.NoFallbackAvailableException;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.order.client.InventoryClient;
import ru.yandex.practicum.order.client.ProductClient;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.exception.DependencyUnavailableException;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.exception.OrderConflictException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Service
@RequiredArgsConstructor
public class OrderAvailabilityService {
    private final ProductClient products;
    private final InventoryClient inventory;

    public void check(List<OrderItemRequest> items) {
        Map<Long, Long> quantities = new LinkedHashMap<>();
        items.forEach(item -> quantities.merge(item.productId(), item.quantity().longValue(), Long::sum));
        quantities.forEach((productId, quantity) -> {
            var product = call(() -> products.getProduct(productId), "Каталог", productId);
            if (product == null || !productId.equals(product.id()) || product.active() == null) {
                throw new DependencyUnavailableException("Некорректный ответ каталога", null);
            }
            if (!product.active()) {
                throw new OrderConflictException("Товар недоступен для заказа: " + productId);
            }
            var stock = call(() -> inventory.getInventory(productId), "Склад", productId);
            if (stock == null || !productId.equals(stock.productId()) || stock.availableQuantity() == null) {
                throw new DependencyUnavailableException("Некорректный ответ склада", null);
            }
            if (stock.availableQuantity() < quantity) {
                throw new OrderConflictException("Недостаточно товара: " + productId);
            }
        });
    }

    private <T> T call(Supplier<T> request, String service, Long productId) {
        try {
            return request.get();
        } catch (NoFallbackAvailableException e) {
            throw translate(e.getCause(), service, productId);
        } catch (FeignException e) {
            throw translate(e, service, productId);
        }
    }

    private RuntimeException translate(Throwable cause, String service, Long productId) {
        if (cause instanceof FeignException.NotFound) {
            return new NotFoundException(service + ": товар не найден: " + productId);
        }
        return new DependencyUnavailableException(service + " временно недоступен", cause);
    }
}
