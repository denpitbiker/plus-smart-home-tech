package ru.yandex.practicum.product.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.product.entity.Category;

public interface CategoryRepository extends JpaRepository<Category, Long> {
}
