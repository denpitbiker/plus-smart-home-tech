package ru.yandex.practicum.order.mapper;

import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemDto;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;

public final class OrderMapper {
    private OrderMapper() {
    }

    public static OrderDto toDto(Order order) {
        return OrderDto.builder()
                .id(order.getId())
                .customerName(order.getCustomerName())
                .customerEmail(order.getCustomerEmail())
                .status(order.getStatus().name())
                .totalPrice(order.getTotalPrice())
                .statusDetails(order.getStatusDetails())
                .createdAt(order.getCreatedAt())
                .items(order.getItems().stream().map(OrderMapper::toDto).toList())
                .build();
    }

    private static OrderItemDto toDto(OrderItem item) {
        return OrderItemDto.builder()
                .id(item.getId())
                .productId(item.getProductId())
                .productName(item.getProductName())
                .quantity(item.getQuantity())
                .price(item.getPrice())
                .build();
    }
}
