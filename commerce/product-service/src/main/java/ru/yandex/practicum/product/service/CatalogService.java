package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CategoryDto;
import ru.yandex.practicum.product.dto.CreateCategoryRequest;
import ru.yandex.practicum.product.dto.CreateProductRequest;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.dto.UpdateProductRequest;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.entity.Product;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.mapper.ProductMapper;
import ru.yandex.practicum.product.repository.CategoryRepository;
import ru.yandex.practicum.product.repository.ProductRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CatalogService {
    private final CategoryRepository categories;
    private final ProductRepository products;

    public List<CategoryDto> getCategories() {
        log.debug("Получение категорий");
        return categories.findAll(Sort.by("id")).stream().map(ProductMapper::toDto).toList();
    }

    public CategoryDto getCategory(Long id) {
        log.debug("Получение категории: id={}", id);
        return ProductMapper.toDto(requireCategory(id));
    }

    @Transactional
    public CategoryDto createCategory(CreateCategoryRequest request) {
        log.info("Создание категории");
        Category category = Category.builder()
                .name(request.name())
                .description(request.description())
                .build();
        return ProductMapper.toDto(categories.save(category));
    }

    public List<ProductDto> getProducts() {
        log.debug("Получение активных товаров");
        return products.findByActiveTrueOrderByIdAsc().stream().map(ProductMapper::toDto).toList();
    }

    public ProductDto getProduct(Long id) {
        log.debug("Получение товара: id={}", id);
        return ProductMapper.toDto(requireProduct(id));
    }

    public List<ProductDto> getProductsByCategory(Long categoryId) {
        log.debug("Получение товаров категории: id={}", categoryId);
        requireCategory(categoryId);
        return products.findByCategoryIdAndActiveTrueOrderByIdAsc(categoryId).stream().map(ProductMapper::toDto).toList();
    }

    public List<ProductDto> search(String query) {
        log.debug("Поиск товаров по названию");
        return products.findByActiveTrueAndNameContainingIgnoreCaseOrderByIdAsc(query).stream()
                .map(ProductMapper::toDto).toList();
    }

    @Transactional
    public ProductDto createProduct(CreateProductRequest request) {
        log.info("Создание товара: categoryId={}", request.categoryId());
        Product product = Product.builder()
                .name(request.name())
                .description(request.description())
                .price(request.price())
                .category(request.categoryId() == null ? null : requireCategory(request.categoryId()))
                .imageUrl(request.imageUrl())
                .build();
        return ProductMapper.toDto(products.save(product));
    }

    @Transactional
    public ProductDto updateProduct(Long id, UpdateProductRequest request) {
        log.info("Обновление товара: id={}", id);
        Product product = requireProduct(id);
        if (request.name() != null) product.setName(request.name());
        if (request.description() != null) product.setDescription(request.description());
        if (request.price() != null) product.setPrice(request.price());
        if (request.categoryId() != null) product.setCategory(requireCategory(request.categoryId()));
        if (request.imageUrl() != null) product.setImageUrl(request.imageUrl());
        if (request.active() != null) product.setActive(request.active());
        return ProductMapper.toDto(product);
    }

    private Category requireCategory(Long id) {
        return categories.findById(id).orElseThrow(() -> new NotFoundException("Категория не найдена: " + id));
    }

    private Product requireProduct(Long id) {
        return products.findById(id).orElseThrow(() -> new NotFoundException("Товар не найден: " + id));
    }
}
