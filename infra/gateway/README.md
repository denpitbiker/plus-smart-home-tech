# Gateway

Gateway работает на порту 8080. Конфигурацию получает из Config Server,
адреса сервисов — из Eureka.

После запуска PostgreSQL, Discovery Server, Config Server и commerce-сервисов:

```bash
mvn -pl infra/gateway -am package
java -jar infra/gateway/target/gateway-1.0-SNAPSHOT.jar
curl http://localhost:8080/products
```

После старта нужно дождаться регистрации сервисов и обновления реестра Eureka
в Gateway. До этого запросы могут возвращать 503.

Маршруты `/products` и `/categories` ведут в `product-service`,
`/inventory` — в `inventory-service`, `/orders` — в `order-service`.
Gateway добавляет к пути `/api`, параметры запроса сохраняются.

Для GET `/products/**` при ответе 404 выполняются до трёх повторных запросов.
В нашем каталоге так обрабатывается `NotFoundException`. Если товар не появился,
клиент получает последний ответ 404. POST и PATCH не повторяются.

Проверка конфигурации: http://localhost:8888/gateway/default.
Тесты: `mvn -pl infra/gateway -am test`.
