package ru.yandex.practicum.product.dto;

import lombok.Builder;

@Builder
public record CategoryDto(

        Long id,

        String name,

        String description
) {
}