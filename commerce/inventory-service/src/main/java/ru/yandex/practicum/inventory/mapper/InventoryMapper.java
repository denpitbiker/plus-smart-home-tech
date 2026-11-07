package ru.yandex.practicum.inventory.mapper;

import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.entity.Inventory;

public final class InventoryMapper {
    private InventoryMapper() {
    }

    public static InventoryDto toDto(Inventory inventory) {
        return new InventoryDto(inventory.getId(), inventory.getProductId(), inventory.getQuantity(),
                inventory.getReservedQuantity(), inventory.getAvailableQuantity());
    }
}
