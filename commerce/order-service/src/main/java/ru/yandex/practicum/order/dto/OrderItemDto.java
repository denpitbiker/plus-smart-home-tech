package ru.yandex.practicum.order.dto;

import lombok.Builder;

import java.math.BigDecimal;

@Builder
public record OrderItemDto(

        Long id,

        Long productId,

        String productName,

        Integer quantity,

        BigDecimal price
) {
}