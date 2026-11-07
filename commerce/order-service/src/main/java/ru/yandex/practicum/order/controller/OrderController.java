package ru.yandex.practicum.order.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.service.OrderService;
import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderService service;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderDto create(@Valid @RequestBody CreateOrderRequest request) {
        return service.create(request);
    }

    @GetMapping("/{id}")
    public OrderDto getById(@PathVariable Long id) {
        return service.getById(id);
    }

    @GetMapping
    public List<OrderDto> getAll() {
        return service.getAll();
    }

    @GetMapping("/by-email")
    public List<OrderDto> getByEmail(@RequestParam String email) {
        return service.getByEmail(email);
    }
}
