CREATE TABLE IF NOT EXISTS transactions (
    client_id STRING,
    transaction_id STRING,
    item_id STRING,
    item_description STRING,
    quantity INT,
    transaction_date TIMESTAMP,
    total_amount DECIMAL(10,2)
) WITH (
    format = 'CSV',
    external_location = 's3://your-bucket/transactions/'
);
