package ru.yandex.practicum.order.mapper;

import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemDto;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;

public final class OrderMapper {
    private OrderMapper() {
    }

    public static OrderDto toDto(Order order) {
        return new OrderDto(order.getId(), order.getCustomerName(), order.getCustomerEmail(), order.getStatus().name(),
                order.getTotalPrice(), order.getStatusDetails(), order.getCreatedAt(),
                order.getItems().stream().map(OrderMapper::toDto).toList());
    }

    private static OrderItemDto toDto(OrderItem item) {
        return new OrderItemDto(item.getId(), item.getProductId(), item.getProductName(), item.getQuantity(), item.getPrice());
    }
}
