package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.order.client.InventoryClient;
import ru.yandex.practicum.order.client.ProductClient;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.exception.DependencyUnavailableException;
import ru.yandex.practicum.order.exception.OrderConflictException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class OrderAvailabilityService {
    private final ProductClient products;
    private final InventoryClient inventory;

    public void check(List<OrderItemRequest> items) {
        Map<Long, Long> quantities = new LinkedHashMap<>();
        items.forEach(item -> quantities.merge(item.productId(), item.quantity().longValue(), Long::sum));
        quantities.forEach((productId, quantity) -> {
            var product = products.getProduct(productId);
            if (product == null || !productId.equals(product.id()) || product.active() == null) {
                throw new DependencyUnavailableException("Некорректный ответ каталога", null);
            }
            if (!product.active()) {
                throw new OrderConflictException("Товар недоступен для заказа: " + productId);
            }
            var stock = inventory.getInventory(productId);
            if (stock == null || !productId.equals(stock.productId()) || stock.availableQuantity() == null) {
                throw new DependencyUnavailableException("Некорректный ответ склада", null);
            }
            if (stock.availableQuantity() < quantity) {
                throw new OrderConflictException("Недостаточно товара: " + productId);
            }
        });
    }

}
