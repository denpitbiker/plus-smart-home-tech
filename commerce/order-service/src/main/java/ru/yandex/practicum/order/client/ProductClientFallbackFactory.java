package ru.yandex.practicum.order.client;

import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

@Component
public class ProductClientFallbackFactory implements FallbackFactory<ProductClient> {
    private static final String SERVICE_NAME = "Каталог";

    @Override
    public ProductClient create(Throwable cause) {
        return productId -> {
            throw ClientExceptionMapper.map(cause, SERVICE_NAME, productId);
        };
    }
}
