# Интернет-магазин

Три сервиса: каталог товаров, склад и заказы. У каждого своя база PostgreSQL.
Настройки находятся в `infra/config-repo`, сервисы регистрируются в Eureka.

Перед созданием заказа `order-service` через OpenFeign проверяет активность товаров
в каталоге и доступное количество на складе. Адреса выбираются через Eureka и
LoadBalancer. Название и цена сохраняются из запроса; проверка остатка не резервирует товар.

Вызовы защищены Resilience4j Circuit Breaker. При недоступности зависимости заказ
не сохраняется, ответ — 503. Отсутствующий товар — 404, неактивный товар или нехватка
остатка — 409. Таймауты и параметры Circuit Breaker заданы в `order-service.yml`.

## Запуск

Нужны Java 21, Maven и Docker.

```bash
mvn clean package
docker compose up -d postgres
```

Затем запустить в отдельных терминалах:

```bash
java -jar infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar
java -jar infra/config-server/target/config-server-1.0-SNAPSHOT.jar
java -jar commerce/product-service/target/product-service-1.0-SNAPSHOT.jar
java -jar commerce/inventory-service/target/inventory-service-1.0-SNAPSHOT.jar
java -jar commerce/order-service/target/order-service-1.0-SNAPSHOT.jar
java -jar infra/api-gateway/target/api-gateway-1.0-SNAPSHOT.jar
java -jar commerce/web-ui/web-ui.jar --spring.profiles.active=gateway-secured
```

Перед запуском commerce нужно дождаться готовности Config Server и Eureka.
Eureka: http://localhost:8761, магазин: http://localhost:8443.
Порты сервисов случайные, их можно посмотреть в логах.

Для отдельной проверки сервисов без Gateway можно использовать режим `direct`:

```bash
java -jar commerce/web-ui/web-ui.jar --spring.profiles.active=direct \
  --app.api.product-base-url=http://localhost:PRODUCT_PORT \
  --app.api.inventory-base-url=http://localhost:INVENTORY_PORT \
  --app.api.order-base-url=http://localhost:ORDER_PORT
```

`PRODUCT_PORT`, `INVENTORY_PORT` и `ORDER_PORT` заменить на порты из логов.
Базы `product_db`, `inventory_db`, `order_db` создаются при первом запуске PostgreSQL.
Если Docker volume уже существовал, недостающие базы нужно создать вручную.

## Тесты

```bash
mvn -pl commerce/product-service,commerce/inventory-service,commerce/order-service test
```

В готовом Web UI оформление заказа пока не работает: jar отправляет только
`productId` и `quantity`, а API по заданию требует ещё `productName` и `price`.
Создание заказа проверено через API. Остальные экраны работают.
