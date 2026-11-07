package ru.yandex.practicum.product;

import ru.yandex.practicum.product.dto.CategoryDto;
import ru.yandex.practicum.product.dto.CreateCategoryRequest;
import ru.yandex.practicum.product.dto.CreateProductRequest;
import ru.yandex.practicum.product.dto.ProductDto;

import java.math.BigDecimal;

final class Stubs {
    static final long PRODUCT_ID = 42L;
    static final long CATEGORY_ID = 7L;

    private Stubs() {
    }

    static CreateCategoryRequest categoryRequest() {
        return new CreateCategoryRequest("Освещение", "Умные лампы");
    }

    static CategoryDto category() {
        return CategoryDto.builder()
                .id(CATEGORY_ID)
                .name(categoryRequest().name())
                .description(categoryRequest().description())
                .build();
    }

    static CreateProductRequest productRequest() {
        return new CreateProductRequest("Лампа", "Умная лампа", new BigDecimal("1490.25"), CATEGORY_ID, null);
    }

    static ProductDto product() {
        return product(true);
    }

    static ProductDto product(boolean active) {
        CreateProductRequest request = productRequest();
        return ProductDto.builder()
                .id(PRODUCT_ID)
                .name(request.name())
                .description(request.description())
                .price(request.price())
                .category(category())
                .active(active)
                .build();
    }
}
