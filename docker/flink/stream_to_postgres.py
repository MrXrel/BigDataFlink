import os

from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def env(name, default):
    return os.environ.get(name, default)


KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", "pet_sales_events")
CONSUMER_GROUP = env("FLINK_CONSUMER_GROUP", "pet-sales-star-schema")

POSTGRES_JDBC_URL = env("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/pet_sales_dw")
POSTGRES_USER = env("POSTGRES_USER", "flink")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", "flink_pass")


def jdbc_options(table_name):
    return f"""
    WITH (
      'connector' = 'jdbc',
      'url' = '{POSTGRES_JDBC_URL}',
      'table-name' = '{table_name}',
      'username' = '{POSTGRES_USER}',
      'password' = '{POSTGRES_PASSWORD}',
      'sink.buffer-flush.max-rows' = '200',
      'sink.buffer-flush.interval' = '1s',
      'sink.max-retries' = '3'
    )
    """


def create_environment():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(environment_settings=settings)
    table_env.get_config().get_configuration().set_string(
        "pipeline.name", "pet-sales-kafka-to-postgres"
    )
    return table_env


def register_source(table_env):
    table_env.execute_sql(
        f"""
        CREATE TABLE raw_sales (
          source_file STRING,
          source_record_no INT,
          id BIGINT,
          customer_first_name STRING,
          customer_last_name STRING,
          customer_age INT,
          customer_email STRING,
          customer_country STRING,
          customer_postal_code STRING,
          customer_pet_type STRING,
          customer_pet_name STRING,
          customer_pet_breed STRING,
          seller_first_name STRING,
          seller_last_name STRING,
          seller_email STRING,
          seller_country STRING,
          seller_postal_code STRING,
          product_name STRING,
          product_category STRING,
          product_price DECIMAL(10, 2),
          product_quantity INT,
          sale_date STRING,
          sale_customer_id INT,
          sale_seller_id INT,
          sale_product_id INT,
          sale_quantity INT,
          sale_total_price DECIMAL(12, 2),
          store_name STRING,
          store_location STRING,
          store_city STRING,
          store_state STRING,
          store_country STRING,
          store_phone STRING,
          store_email STRING,
          pet_category STRING,
          product_weight DECIMAL(8, 2),
          product_color STRING,
          product_size STRING,
          product_brand STRING,
          product_material STRING,
          product_description STRING,
          product_rating DECIMAL(4, 2),
          product_reviews INT,
          product_release_date STRING,
          product_expiry_date STRING,
          supplier_name STRING,
          supplier_contact STRING,
          supplier_email STRING,
          supplier_phone STRING,
          supplier_address STRING,
          supplier_city STRING,
          supplier_country STRING,
          processed_at AS PROCTIME()
        )
        WITH (
          'connector' = 'kafka',
          'topic' = '{KAFKA_TOPIC}',
          'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
          'properties.group.id' = '{CONSUMER_GROUP}',
          'scan.startup.mode' = 'earliest-offset',
          'format' = 'json',
          'json.fail-on-missing-field' = 'false',
          'json.ignore-parse-errors' = 'true'
        )
        """
    )


def register_sinks(table_env):
    table_env.execute_sql(
        f"""
        CREATE TABLE jdbc_dim_dates (
          date_id INT,
          full_date DATE,
          sale_year INT,
          sale_month INT,
          sale_day INT,
          sale_quarter INT,
          PRIMARY KEY (date_id) NOT ENFORCED
        )
        {jdbc_options("dim_dates")}
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE jdbc_dim_customers (
          customer_id INT,
          first_name STRING,
          last_name STRING,
          age INT,
          email STRING,
          country STRING,
          postal_code STRING,
          pet_type STRING,
          pet_name STRING,
          pet_breed STRING,
          PRIMARY KEY (customer_id) NOT ENFORCED
        )
        {jdbc_options("dim_customers")}
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE jdbc_dim_sellers (
          seller_id INT,
          first_name STRING,
          last_name STRING,
          email STRING,
          country STRING,
          postal_code STRING,
          PRIMARY KEY (seller_id) NOT ENFORCED
        )
        {jdbc_options("dim_sellers")}
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE jdbc_dim_stores (
          store_name STRING,
          location STRING,
          city STRING,
          state STRING,
          country STRING,
          phone STRING,
          email STRING,
          PRIMARY KEY (store_name) NOT ENFORCED
        )
        {jdbc_options("dim_stores")}
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE jdbc_dim_suppliers (
          supplier_name STRING,
          contact_name STRING,
          email STRING,
          phone STRING,
          address STRING,
          city STRING,
          country STRING,
          PRIMARY KEY (supplier_name) NOT ENFORCED
        )
        {jdbc_options("dim_suppliers")}
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE jdbc_dim_products (
          product_id INT,
          product_name STRING,
          category STRING,
          brand STRING,
          price DECIMAL(10, 2),
          stock_quantity INT,
          pet_category STRING,
          weight DECIMAL(8, 2),
          color STRING,
          size STRING,
          material STRING,
          description STRING,
          rating DECIMAL(4, 2),
          reviews INT,
          release_date DATE,
          expiry_date DATE,
          supplier_name STRING,
          PRIMARY KEY (product_id) NOT ENFORCED
        )
        {jdbc_options("dim_products")}
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE jdbc_fact_sales (
          source_file STRING,
          source_record_no INT,
          source_row_id BIGINT,
          date_id INT,
          customer_id INT,
          seller_id INT,
          product_id INT,
          store_name STRING,
          supplier_name STRING,
          quantity INT,
          total_price DECIMAL(12, 2)
        )
        {jdbc_options("fact_sales")}
        """
    )


def add_pipeline_inserts(table_env):
    statement_set = table_env.create_statement_set()

    statement_set.add_insert_sql(
        """
        INSERT INTO jdbc_dim_dates
        SELECT
          CAST(EXTRACT(YEAR FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT) * 10000
            + CAST(EXTRACT(MONTH FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT) * 100
            + CAST(EXTRACT(DAY FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT),
          TO_DATE(sale_date, 'M/d/yyyy'),
          CAST(EXTRACT(YEAR FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT),
          CAST(EXTRACT(MONTH FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT),
          CAST(EXTRACT(DAY FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT),
          CAST(CEIL(EXTRACT(MONTH FROM TO_DATE(sale_date, 'M/d/yyyy')) / 3.0) AS INT)
        FROM raw_sales
        WHERE sale_date IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO jdbc_dim_customers
        SELECT
          sale_customer_id,
          customer_first_name,
          customer_last_name,
          customer_age,
          customer_email,
          customer_country,
          customer_postal_code,
          customer_pet_type,
          customer_pet_name,
          customer_pet_breed
        FROM raw_sales
        WHERE sale_customer_id IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO jdbc_dim_sellers
        SELECT
          sale_seller_id,
          seller_first_name,
          seller_last_name,
          seller_email,
          seller_country,
          seller_postal_code
        FROM raw_sales
        WHERE sale_seller_id IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO jdbc_dim_stores
        SELECT
          store_name,
          store_location,
          store_city,
          store_state,
          store_country,
          store_phone,
          store_email
        FROM raw_sales
        WHERE store_name IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO jdbc_dim_suppliers
        SELECT
          supplier_name,
          supplier_contact,
          supplier_email,
          supplier_phone,
          supplier_address,
          supplier_city,
          supplier_country
        FROM raw_sales
        WHERE supplier_name IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO jdbc_dim_products
        SELECT
          sale_product_id,
          product_name,
          product_category,
          product_brand,
          product_price,
          product_quantity,
          pet_category,
          product_weight,
          product_color,
          product_size,
          product_material,
          product_description,
          product_rating,
          product_reviews,
          TO_DATE(product_release_date, 'M/d/yyyy'),
          TO_DATE(product_expiry_date, 'M/d/yyyy'),
          supplier_name
        FROM raw_sales
        WHERE sale_product_id IS NOT NULL
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO jdbc_fact_sales
        SELECT
          source_file,
          source_record_no,
          id,
          CAST(EXTRACT(YEAR FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT) * 10000
            + CAST(EXTRACT(MONTH FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT) * 100
            + CAST(EXTRACT(DAY FROM TO_DATE(sale_date, 'M/d/yyyy')) AS INT),
          sale_customer_id,
          sale_seller_id,
          sale_product_id,
          store_name,
          supplier_name,
          sale_quantity,
          sale_total_price
        FROM raw_sales
        WHERE sale_date IS NOT NULL
          AND sale_customer_id IS NOT NULL
          AND sale_seller_id IS NOT NULL
          AND sale_product_id IS NOT NULL
        """
    )

    return statement_set


def main():
    table_env = create_environment()
    register_source(table_env)
    register_sinks(table_env)
    job = add_pipeline_inserts(table_env)
    print("Submitting streaming job: Kafka pet sales -> PostgreSQL star schema")
    job.execute()


if __name__ == "__main__":
    main()
