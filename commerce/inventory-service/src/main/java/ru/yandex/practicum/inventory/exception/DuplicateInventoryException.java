package ru.yandex.practicum.inventory.exception;

public class DuplicateInventoryException extends RuntimeException {
    public DuplicateInventoryException(Long productId) {
        super("Складская запись уже существует для товара: " + productId);
    }
}
