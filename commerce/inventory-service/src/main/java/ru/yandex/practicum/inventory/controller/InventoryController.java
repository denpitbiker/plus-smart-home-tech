package ru.yandex.practicum.inventory.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.service.InventoryService;

import java.util.List;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
public class InventoryController {
    private final InventoryService service;

    @GetMapping
    public ResponseEntity<List<InventoryDto>> getAll() {
        return ResponseEntity.ok(service.getAll());
    }

    @GetMapping("/{productId}")
    public ResponseEntity<InventoryDto> getByProductId(@PathVariable Long productId) {
        return ResponseEntity.ok(service.getByProductId(productId));
    }

    @PostMapping
    public ResponseEntity<InventoryDto> create(@Valid @RequestBody UpdateInventoryRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(service.create(request));
    }

    @PutMapping
    public ResponseEntity<InventoryDto> update(@Valid @RequestBody UpdateInventoryRequest request) {
        return ResponseEntity.ok(service.update(request));
    }

    @PostMapping("/reserve")
    public ResponseEntity<ReserveResponse> reserve(@Valid @RequestBody ReserveRequest request) {
        return ResponseEntity.ok(service.reserve(request));
    }
}
