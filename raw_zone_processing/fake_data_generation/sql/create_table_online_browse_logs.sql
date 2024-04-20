CREATE TABLE IF NOT EXISTS "ecommerce_broswer_logs"."ecomm_logs".browsing_data (
    ip VARCHAR,
    ts TIMESTAMP,
    tz VARCHAR,
    verb VARCHAR,
    resource_type VARCHAR,
    resource_fk VARCHAR,
    response INTEGER,
    browser VARCHAR,
    os VARCHAR,
    customer BIGINT,
    d_day_name VARCHAR,
    i_current_price DOUBLE,
    i_category VARCHAR,
    i_description VARCHAR,
    c_preferred_cust_flag BOOLEAN,
    ds DATE
) WITH (
    format = 'Parquet',
    external_location = 's3a://ecommlogs/ecomm_logs'
);
