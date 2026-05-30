# Проверка лабораторной работы

Инструкция рассчитана на проверку из корня проекта `BigDataFlink`.

## 1. Запустить инфраструктуру

```bash
cd ~/BigDataFlink
docker compose up -d --build
```

Проверить, что контейнеры поднялись:

```bash
docker compose ps
```

## 2. Запустить Flink job

```bash
docker compose exec jobmanager flink run -py /opt/flink/jobs/stream_to_postgres.py
```

Flink Web UI:

```text
http://localhost:8081
```

## 3. Проверить PostgreSQL

Проверить количество фактов:

```bash
docker compose exec postgres psql -U flink -d pet_sales_dw -c "SELECT count(*) FROM fact_sales;"
```

Ожидаемый результат после завершения обработки:

```text
10000
```

Если число меньше, подождать 10-30 секунд и повторить запрос.

Проверить таблицы измерений:

```bash
docker compose exec postgres psql -U flink -d pet_sales_dw -c "SELECT count(*) FROM dim_customers;"
docker compose exec postgres psql -U flink -d pet_sales_dw -c "SELECT count(*) FROM dim_products;"
docker compose exec postgres psql -U flink -d pet_sales_dw -c "SELECT count(*) FROM dim_sellers;"
docker compose exec postgres psql -U flink -d pet_sales_dw -c "SELECT count(*) FROM dim_stores;"
docker compose exec postgres psql -U flink -d pet_sales_dw -c "SELECT count(*) FROM dim_suppliers;"
docker compose exec postgres psql -U flink -d pet_sales_dw -c "SELECT count(*) FROM dim_dates;"
```

Посмотреть пример витрины:

```bash
docker compose exec postgres psql -U flink -d pet_sales_dw -c "
SELECT
    f.sale_id,
    d.full_date,
    c.first_name AS customer_name,
    c.last_name AS customer_last_name,
    p.product_name,
    p.category,
    f.quantity,
    f.total_price,
    f.store_name,
    f.supplier_name
FROM fact_sales f
LEFT JOIN dim_dates d ON d.date_id = f.date_id
LEFT JOIN dim_customers c ON c.customer_id = f.customer_id
LEFT JOIN dim_products p ON p.product_id = f.product_id
ORDER BY f.sale_id
LIMIT 10;"
```

