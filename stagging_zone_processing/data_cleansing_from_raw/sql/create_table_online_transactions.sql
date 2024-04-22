CREATE TABLE ecommerce_transactions.ecomm_trans.transactions (
    client_id BIGINT,
    transaction_id VARCHAR,
    item_id VARCHAR,
    item_description VARCHAR,
    category VARCHAR,
    quantity INTEGER,
    total_amount DOUBLE,
    credit_card_number VARCHAR,
    transaction_date TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://ecommtrans/ecomm_trans/',
    partitioned_by = ARRAY['transaction_date']
);
