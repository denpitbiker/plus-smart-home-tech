package ru.yandex.practicum.product.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.product.dto.CreateProductRequest;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.dto.UpdateProductRequest;
import ru.yandex.practicum.product.service.CatalogService;

import java.util.List;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class ProductController {
    private final CatalogService service;

    @GetMapping
    public List<ProductDto> getAll() {
        return service.getProducts();
    }

    @GetMapping("/{id}")
    public ProductDto getById(@PathVariable Long id) {
        return service.getProduct(id);
    }

    @GetMapping("/category/{categoryId}")
    public List<ProductDto> getByCategory(@PathVariable Long categoryId) {
        return service.getProductsByCategory(categoryId);
    }

    @GetMapping("/search")
    public List<ProductDto> search(@RequestParam String query) {
        return service.search(query);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto create(@Valid @RequestBody CreateProductRequest request) {
        return service.createProduct(request);
    }

    @PatchMapping("/{id}")
    public ProductDto update(@PathVariable Long id, @Valid @RequestBody UpdateProductRequest request) {
        return service.updateProduct(id, request);
    }
}
