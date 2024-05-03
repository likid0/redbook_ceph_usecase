CREATE TABLE transactions (
    "Client ID" bigint,
    "Transaction ID" varchar,
    "Item ID" varchar,
    "Item Description" varchar,
    "Category" varchar,
    "Quantity" integer,
    "Total Amount" double,
    "Credit Card Number" varchar,
    "Transaction Date" timestamp
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://ecommtrans/transactions/'
);
