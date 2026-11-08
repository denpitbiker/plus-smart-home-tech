package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.mapper.OrderMapper;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {
    private final OrderRepository repository;
    private final OrderAvailabilityService availability;

    @Transactional
    public OrderDto create(CreateOrderRequest request) {
        log.info("Создание заказа: позиций={}", request.items().size());
        availability.check(request.items());
        Order order = Order.builder()
                .customerName(request.customerName())
                .customerEmail(request.customerEmail())
                .build();
        BigDecimal total = BigDecimal.ZERO;
        for (OrderItemRequest requestedItem : request.items()) {
            OrderItem item = OrderItem.builder()
                    .productId(requestedItem.productId())
                    .productName(requestedItem.productName())
                    .quantity(requestedItem.quantity())
                    .price(requestedItem.price())
                    .build();
            order.addItem(item);
            total = total.add(requestedItem.price().multiply(BigDecimal.valueOf(requestedItem.quantity())));
        }
        order.setTotalPrice(total);
        return OrderMapper.toDto(repository.save(order));
    }

    public OrderDto getById(Long id) {
        log.debug("Получение заказа: id={}", id);
        return OrderMapper.toDto(repository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказ не найден: " + id)));
    }

    public List<OrderDto> getAll() {
        log.debug("Получение всех записей");
        return repository.findAll(Sort.by("id")).stream().map(OrderMapper::toDto).toList();
    }

    public List<OrderDto> getByEmail(String email) {
        log.debug("Поиск заказов по email");
        return repository.findByCustomerEmailOrderByIdAsc(email).stream().map(OrderMapper::toDto).toList();
    }
}
