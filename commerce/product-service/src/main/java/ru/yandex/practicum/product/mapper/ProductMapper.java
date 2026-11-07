package ru.yandex.practicum.product.mapper;

import ru.yandex.practicum.product.dto.CategoryDto;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.entity.Product;

public final class ProductMapper {
    private ProductMapper() {
    }

    public static CategoryDto toDto(Category category) {
        return category == null ? null : new CategoryDto(category.getId(), category.getName(), category.getDescription());
    }

    public static ProductDto toDto(Product product) {
        return new ProductDto(product.getId(), product.getName(), product.getDescription(), product.getPrice(),
                toDto(product.getCategory()), product.getImageUrl(), product.isActive());
    }
}
