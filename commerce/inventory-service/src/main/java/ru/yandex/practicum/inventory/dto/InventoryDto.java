package ru.yandex.practicum.inventory.dto;

import lombok.Builder;

@Builder
public record InventoryDto(

        Long id,

        Long productId,

        Integer quantity,

        Integer reservedQuantity,

        Integer availableQuantity
) {
}
