# API Gateway

Работает на порту 8080, получает конфигурацию из Config Server и адреса сервисов из Eureka.
После запуска PostgreSQL, Discovery Server, Config Server и commerce-сервисов:

```bash
mvn -pl infra/api-gateway -am package
java -jar infra/api-gateway/target/api-gateway-1.0-SNAPSHOT.jar
java -jar commerce/web-ui/web-ui.jar --spring.profiles.active=gateway-secured
```

Нужно дождаться обновления реестра Eureka, иначе возможен ответ 503.
Конфигурация: http://localhost:8888/api-gateway/default.

GET `/api/products/**`, `/api/categories/**`, `/api/inventory/**` доступны всем.
`ivan:ivan` может создавать заказы и читать их по ID или email.
`anna:anna` также может изменять каталог и склад и читать общий список заказов.
Без переменных окружения используются BCrypt-хеши учебных паролей из конфигурации.
`SECURITY_IVAN_PASSWORD` и `SECURITY_ANNA_PASSWORD` позволяют переопределить пароли при запуске.
Пользователи и CORS задаются во внешней конфигурации `app.security` и `globalcors`.
Публичный health-check: `GET /actuator/health`, без диагностических деталей.
Таймаут подключения к сервисам — 1 секунда, ответа — 10 секунд.
Неизвестные маршруты закрыты, старые пути без `/api` больше не используются.

```bash
curl http://localhost:8080/api/products
curl -u anna:anna http://localhost:8080/api/orders
mvn -pl infra/api-gateway -am test
```

Swagger: `http://localhost:8080/product-service/swagger-ui.html`.
Для склада и заказов заменить `product-service` на `inventory-service` или `order-service`.
Документация публичная. GET товаров при 404 повторяется до трёх раз; запись не повторяется.

Web UI доступен на http://localhost:8443. Его jar отправляет неполный DTO заказа:
после успешной авторизации оформление может вернуть 400. Проверить создание заказа
можно через Gateway, передав в каждой позиции `productId`, `productName`, `quantity`, `price`.
