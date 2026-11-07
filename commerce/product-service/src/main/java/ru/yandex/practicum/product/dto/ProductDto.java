package ru.yandex.practicum.product.dto;

import lombok.Builder;

import java.math.BigDecimal;

@Builder
public record ProductDto(

        Long id,

        String name,

        String description,

        BigDecimal price,

        CategoryDto category,

        String imageUrl,

        Boolean active
) {
}