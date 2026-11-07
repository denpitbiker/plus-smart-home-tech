# Интернет-магазин: локальный запуск

Требуются Java 21, Maven 3.9 и PostgreSQL 16 (либо Docker Compose).
Команды ниже выполняются из корня репозитория. Каждый Java-процесс запускается
в отдельном терминале. Все бизнес-сервисы используют случайные свободные порты.

## Сборка и тесты

```bash
mvn -pl commerce/product-service,commerce/inventory-service,commerce/order-service test
mvn verify
```

Acceptance-тесты используют H2 без Config Server и Eureka. Дополнительные тесты
проверяют частичные обновления, снимки, дробные цены, ошибки и конкурентные
операции склада. H2 для денежных полей работает в PostgreSQL-режиме.

## Базы данных

```bash
docker compose up -d postgres
```

При первом запуске `docker/init-databases.sql` создаёт `product_db`,
`inventory_db`, `order_db`. Существующий volume не удаляйте: init-скрипт повторно
не выполняется. Проверьте базы командой:

```bash
docker compose exec postgres psql -U dbuser -d postgres -c '\l'
```

Если нужной базы нет, создайте только её, например:

```bash
docker compose exec postgres createdb -U dbuser product_db
```

Без Docker можно создать эти же три базы в локальном PostgreSQL и передать
учётные данные через переменные окружения. Сервисы сами создают/обновляют свои
таблицы (`ddl-auto: update`); демонстрационные данные не загружаются.

## Config Server, Eureka и сервисы

```bash
java -jar infra/config-server/target/config-server-1.0-SNAPSHOT.jar
java -jar infra/discovery-server/target/discovery-server-1.0-SNAPSHOT.jar
java -jar commerce/product-service/target/product-service-1.0-SNAPSHOT.jar
java -jar commerce/inventory-service/target/inventory-service-1.0-SNAPSHOT.jar
java -jar commerce/order-service/target/order-service-1.0-SNAPSHOT.jar
```

Дождитесь готовности обоих инфраструктурных приложений перед запуском commerce.
Config Server может временно сообщать о недоступности Eureka до её запуска.

Конфиги из `infra/config-repo` включаются в jar Config Server при сборке.
После изменения конфигурации пересоберите и перезапустите Config Server,
затем соответствующий клиент. Перед пересборкой остановите процессы,
запущенные из jar в `target`, либо запускайте отдельные копии jar.

Проверьте `propertySources`:

- http://localhost:8888/product-service/default
- http://localhost:8888/inventory-service/default
- http://localhost:8888/order-service/default

В каждом ответе должны присутствовать общий `config-repo/application.yml`
и соответствующий `config-repo/commerce/*-service.yml`.
В http://localhost:8761 должны появиться `PRODUCT-SERVICE`, `INVENTORY-SERVICE`,
`ORDER-SERVICE` со статусом `UP`. Регистрация может занять несколько секунд.
Фактические порты видны в логах запуска и в `GET /eureka/apps` с заголовком
`Accept: application/json`.

Настройки окружения:

| Переменная | По умолчанию |
| --- | --- |
| `CONFIG_SERVER_URL` | `http://localhost:8888` |
| `EUREKA_SERVER_URL` | `http://localhost:8761/eureka/` |
| `EUREKA_INSTANCE_HOSTNAME` | `localhost` |
| `PRODUCT_DB_URL` | `jdbc:postgresql://localhost:5432/product_db` |
| `INVENTORY_DB_URL` | `jdbc:postgresql://localhost:5432/inventory_db` |
| `ORDER_DB_URL` | `jdbc:postgresql://localhost:5432/order_db` |
| `{PRODUCT,INVENTORY,ORDER}_DB_USERNAME` | `dbuser` |
| `{PRODUCT,INVENTORY,ORDER}_DB_PASSWORD` | `12345` |

DB-переменные задаются в окружении соответствующего бизнес-сервиса.
При запуске на разных машинах `EUREKA_INSTANCE_HOSTNAME` должен содержать
адрес конкретного сервиса, доступный Web UI. Локальные `application.yml`
содержат только имя приложения и обязательный импорт Config Server.

## Web UI

Основной запуск получает адреса сервисов из Eureka:

```bash
java -jar commerce/web-ui/web-ui.jar --spring.profiles.active=discovery
```

Откройте http://localhost:8443. Через админ-панель создайте категорию, товар и
складскую запись. Проверьте каталог, добавление в корзину, оформление заказа
и поиск заказа по email. Затем измените название/цену товара: старый заказ
должен сохранить исходный снимок.

Для отдельной проверки `direct` остановите UI и подставьте фактические порты:

```bash
java -jar commerce/web-ui/web-ui.jar --spring.profiles.active=direct \
  --app.api.product-base-url=http://localhost:<product-port> \
  --app.api.inventory-base-url=http://localhost:<inventory-port> \
  --app.api.order-base-url=http://localhost:<order-port>
```

Повторите пользовательские сценарии. В `direct` UI не использует Eureka,
но сервисы продолжают регистрироваться в ней. После перезапуска сервиса
его порт может измениться — обновите аргумент UI.

## Проверка API и границы версии

```bash
python3 scripts/commerce-smoke.py
```

Smoke-проверка требует запущенные Config Server, Eureka и три сервиса.
Она создаёт уникальные тестовые данные, проверяет источники конфигурации,
снимок заказа после изменения каталога, точность денег и резервирование.
Тестовые записи остаются в локальных БД для проверки после перезапуска.

- Каталог возвращает только активные товары в списках, поиске и категории;
  чтение по ID возвращает также неактивный товар. PATCH игнорирует `null`.
- Категория товара необязательна; неизвестный переданный ID — `404`.
- Склад не проверяет товар в каталоге. Дубликат и нехватка — `409`, попытка
  уменьшить количество ниже резерва — `400`, отсутствующая запись — `404`.
- Резервирование и PUT защищены optimistic locking. При конфликте возвращается
  `409`; клиент может перечитать состояние и решить, повторять ли запрос.
- Заказ сохраняет переданные название/цену и вычисляет сумму через `BigDecimal`.
  Он не проверяет каталог и не резервирует склад автоматически.
- Все ошибки сохраняют поля `status`, `message`, `timestamp`, `validationErrors`.
- Каждый сервис использует только собственную базу. Межсервисных вызовов нет.

## Ограничение поставленного Web UI

В jar из текущего прекода (он совпадает с официальным `main` курса) DTO
`OrderItemRequest` содержит только `productId` и `quantity`. Поэтому кнопка
оформления отправляет запрос без обязательных `productName` и `price`,
и order-service корректно возвращает `400`. Это проявляется в обоих профилях UI.
Для полного сценария оформления требуется обновлённый jar, передающий снимок
названия и цены из корзины. Backend не обращается в каталог за этими данными
и не ослабляет валидацию для обхода несовместимости.

Категории, товары, склад, корзина и просмотр заказов доступны через текущий UI.
Создание заказа проверяется API-командой выше; созданный заказ можно найти
в UI по email, который smoke-тест формирует из напечатанного суффикса товара.
