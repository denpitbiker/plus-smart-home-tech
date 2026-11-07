package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
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

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {
    private final OrderRepository repository;

    @Transactional
    public OrderDto create(CreateOrderRequest request) {
        Order order = new Order();
        order.setCustomerName(request.customerName());
        order.setCustomerEmail(request.customerEmail());
        BigDecimal total = BigDecimal.ZERO;
        for (OrderItemRequest requestedItem : request.items()) {
            OrderItem item = new OrderItem();
            item.setProductId(requestedItem.productId());
            item.setProductName(requestedItem.productName());
            item.setQuantity(requestedItem.quantity());
            item.setPrice(requestedItem.price());
            order.addItem(item);
            total = total.add(requestedItem.price().multiply(BigDecimal.valueOf(requestedItem.quantity())));
        }
        order.setTotalPrice(total);
        return OrderMapper.toDto(repository.save(order));
    }

    public OrderDto getById(Long id) {
        return OrderMapper.toDto(repository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказ не найден: " + id)));
    }

    public List<OrderDto> getAll() {
        return repository.findAll(Sort.by("id")).stream().map(OrderMapper::toDto).toList();
    }

    public List<OrderDto> getByEmail(String email) {
        return repository.findByCustomerEmailOrderByIdAsc(email).stream().map(OrderMapper::toDto).toList();
    }
}
