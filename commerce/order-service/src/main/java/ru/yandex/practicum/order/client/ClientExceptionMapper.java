package ru.yandex.practicum.order.client;

import feign.FeignException;
import ru.yandex.practicum.order.exception.DependencyUnavailableException;
import ru.yandex.practicum.order.exception.NotFoundException;

final class ClientExceptionMapper {
    private ClientExceptionMapper() {
    }

    static RuntimeException map(Throwable cause, String service, Long productId) {
        if (cause instanceof FeignException.NotFound) {
            return new NotFoundException(service + ": товар не найден: " + productId);
        }
        return new DependencyUnavailableException(service + " временно недоступен", cause);
    }
}
