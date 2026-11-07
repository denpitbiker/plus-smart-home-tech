package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.Inventory;
import ru.yandex.practicum.inventory.exception.DuplicateInventoryException;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.mapper.InventoryMapper;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class InventoryService {
    private final InventoryRepository repository;

    public List<InventoryDto> getAll() {
        log.debug("Получение всех записей");
        return repository.findAll(Sort.by("id")).stream().map(InventoryMapper::toDto).toList();
    }

    public InventoryDto getByProductId(Long productId) {
        log.debug("Получение остатка: productId={}", productId);
        return InventoryMapper.toDto(requireInventory(productId));
    }

    @Transactional
    public InventoryDto create(UpdateInventoryRequest request) {
        log.info("Создание складской записи: productId={}, quantity={}", request.productId(), request.quantity());
        if (repository.existsByProductId(request.productId())) {
            throw new DuplicateInventoryException(request.productId());
        }
        Inventory inventory = Inventory.builder()
                .productId(request.productId())
                .quantity(request.quantity())
                .build();
        return InventoryMapper.toDto(repository.saveAndFlush(inventory));
    }

    @Transactional
    public InventoryDto update(UpdateInventoryRequest request) {
        log.info("Обновление остатка: productId={}, quantity={}", request.productId(), request.quantity());
        Inventory inventory = requireInventory(request.productId());
        if (request.quantity() < inventory.getReservedQuantity()) {
            throw new IllegalArgumentException("Общее количество не может быть меньше зарезервированного");
        }
        inventory.setQuantity(request.quantity());
        repository.flush();
        return InventoryMapper.toDto(inventory);
    }

    @Transactional
    public ReserveResponse reserve(ReserveRequest request) {
        log.info("Резервирование товара: productId={}, quantity={}", request.productId(), request.quantity());
        Inventory inventory = requireInventory(request.productId());
        if (request.quantity() > inventory.getAvailableQuantity()) {
            throw new InsufficientStockException("Недостаточно товара: " + request.productId());
        }
        inventory.setReservedQuantity(inventory.getReservedQuantity() + request.quantity());
        repository.flush();
        return ReserveResponse.builder()
                .success(true)
                .availableQuantity(inventory.getAvailableQuantity())
                .message("Товар успешно зарезервирован")
                .build();
    }

    private Inventory requireInventory(Long productId) {
        return repository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException("Складская запись не найдена: " + productId));
    }
}
