CREATE TABLE IF NOT EXISTS browsing_logs (
    ip STRING,
    ts TIMESTAMP,
    tz STRING,
    verb STRING,
    resource_type STRING,
    resource_fk STRING,
    response INT,
    browser STRING,
    os STRING,
    customer STRING,
    d_day_name STRING,
    i_current_price DECIMAL(10,2),
    i_category STRING,
    c_preferred_cust_flag STRING,
    ds STRING
) WITH (
    format = 'CSV',
    external_location = 's3://your-bucket/browsing_logs/',
    partitioned_by = ARRAY['ds']
);
