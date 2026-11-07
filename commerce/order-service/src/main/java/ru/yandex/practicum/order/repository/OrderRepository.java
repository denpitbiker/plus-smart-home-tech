package ru.yandex.practicum.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.order.entity.Order;

import java.util.List;

public interface OrderRepository extends JpaRepository<Order, Long> {
    List<Order> findByCustomerEmailOrderByIdAsc(String email);
}
