package ru.yandex.practicum.order.client;

import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

@Component
public class InventoryClientFallbackFactory implements FallbackFactory<InventoryClient> {
    private static final String SERVICE_NAME = "Склад";

    @Override
    public InventoryClient create(Throwable cause) {
        return productId -> {
            throw ClientExceptionMapper.map(cause, SERVICE_NAME, productId);
        };
    }
}
