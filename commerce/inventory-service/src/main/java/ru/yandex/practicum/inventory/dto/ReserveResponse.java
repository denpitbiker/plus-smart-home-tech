package ru.yandex.practicum.inventory.dto;

import lombok.Builder;

@Builder
public record ReserveResponse(

        boolean success,

        Integer availableQuantity,

        String message
) {
}
