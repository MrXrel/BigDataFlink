# BigDataFlink

Лабораторная работа по потоковой обработке: CSV-файлы с продажами отправляются в Kafka как JSON, затем Apache Flink читает поток и раскладывает данные в PostgreSQL по модели звезды.

Реализация сделана самостоятельной: данные не копируются в Docker-образ producer, поля приводятся к числовым типам до отправки в Kafka, а в витрине добавлена отдельная таблица дат.

## Состав

- `docker-compose.yml` - Kafka, PostgreSQL, Flink JobManager/TaskManager и producer.
- `producer/csv_to_kafka.py` - читает все CSV из папки `исходные данные` и публикует события в Kafka topic `pet_sales_events`.
- `docker/flink/stream_to_postgres.py` - PyFlink streaming job, который читает Kafka и пишет измерения/факты в PostgreSQL.
- `sql/001_star_schema.sql` - DDL для таблиц звезды.
- `исходные данные/` - 10 CSV-файлов по 1000 строк.

## Запуск

Собрать и поднять сервисы:

```bash
docker compose up -d --build
```

Producer стартует вместе с compose и отправляет CSV-строки в Kafka. Если нужно отправить данные повторно:

```bash
docker compose up producer
```

Запустить Flink job:

```bash
docker compose exec jobmanager flink run -py /opt/flink/jobs/stream_to_postgres.py
```

Flink UI доступен здесь:

```text
http://localhost:8081
```

## Проверка PostgreSQL

Подключиться к базе:

```bash
docker compose exec postgres psql -U flink -d pet_sales_dw
```

Проверить количество записей:

```sql
SELECT count(*) FROM fact_sales;
SELECT count(*) FROM dim_customers;
SELECT count(*) FROM dim_products;
SELECT count(*) FROM dim_dates;
```

Посмотреть пример собранной витрины:

```sql
SELECT
    f.sale_id,
    d.full_date,
    c.first_name AS customer_name,
    p.product_name,
    s.store_name,
    f.quantity,
    f.total_price
FROM fact_sales f
LEFT JOIN dim_dates d ON d.date_id = f.date_id
LEFT JOIN dim_customers c ON c.customer_id = f.customer_id
LEFT JOIN dim_products p ON p.product_id = f.product_id
LEFT JOIN dim_stores s ON s.store_name = f.store_name
ORDER BY f.sale_id
LIMIT 20;
```
