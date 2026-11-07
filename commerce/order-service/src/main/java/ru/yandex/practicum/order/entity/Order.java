package ru.yandex.practicum.order.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "orders", indexes = @Index(name = "idx_orders_customer_email", columnList = "customer_email"))
@Getter
@Setter
@NoArgsConstructor
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false, columnDefinition = "text")
    private String customerName;
    @Column(name = "customer_email", nullable = false, columnDefinition = "text")
    private String customerEmail;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status = OrderStatus.CREATED;
    @Column(nullable = false, columnDefinition = "numeric")
    private BigDecimal totalPrice;
    private String statusDetails;
    @Column(nullable = false)
    private LocalDateTime createdAt = LocalDateTime.now();
    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    @OrderBy("id ASC")
    private List<OrderItem> items = new ArrayList<>();

    public void addItem(OrderItem item) {
        items.add(item);
        item.setOrder(this);
    }
}
