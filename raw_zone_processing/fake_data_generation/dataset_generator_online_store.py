import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import calendar
import argparse
import boto3
import logging
import os
import requests
from collections import defaultdict

# Initialize Faker
fake = Faker()

product_names_by_category = {
    "Clothing": ["T-shirt", "Jeans", "Dress", "Jacket", "Sweater", "Skirt", "Scarf", "Gloves", "Socks", "Hat", "Coat", "Blouse", "Pants", "Hoodie", "Pajamas"],
    "Accessories": ["Belt", "Bag", "Watch", "Hat", "Scarf", "Gloves", "Socks", "Tie", "Wallet", "Backpack", "Bracelet", "Earrings", "Necklace", "Ring", "Briefcase"],
    "Footwear": ["Sneakers", "Shoes", "Boots", "Sandals", "Slippers"],
    "Electronics": ["Phone", "Laptop", "Tablet", "Smartwatch", "Headphones", "Speaker", "Camera", "Charger", "Power Bank", "Mouse", "Keyboard", "Monitor", "TV"],
    "Jewelry": ["Ring", "Necklace", "Earrings", "Bracelet", "Pendant", "Brooch", "Chain", "Cufflinks", "Anklet", "Charm", "Choker", "Pin", "Tiara", "Watch"]
}

REQUIRED_ENV_VARS = [
    'S3_ENDPOINT_URL', 'STS_ENDPOINT_URL', 'SOURCE_ROLE_ARN',
    'OIDC_PROVIDER_URL', 'OIDC_CLIENT_ID', 'OIDC_CLIENT_SECRET',
    'OIDC_USERNAME', 'OIDC_PASSWORD', 'S3_BUCKET_NAME'
]

def check_env_variables():
    missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing_vars:
        logging.error("The following environment variables are missing:")
        for var in missing_vars:
            logging.error(var)
        return False
    return True

def get_jwt_token():
    if not check_env_variables():
        return None
    provider_url = os.getenv('OIDC_PROVIDER_URL')
    client_id = os.getenv('OIDC_CLIENT_ID')
    client_secret = os.getenv('OIDC_CLIENT_SECRET')
    username = os.getenv('OIDC_USERNAME')
    password = os.getenv('OIDC_PASSWORD')
    token_endpoint = f"{provider_url}/token"
    payload = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
        'username': username,
        'password': password
    }
    try:
        response = requests.post(token_endpoint, data=payload)
        response.raise_for_status()
        return response.json()['access_token']
    except Exception as e:
        logging.error(f"Error obtaining JWT token: {e}")
        return None

def upload_to_s3(file_name, bucket_name):
    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    sts_endpoint_url = os.getenv('STS_ENDPOINT_URL')
    role_arn = os.getenv('SOURCE_ROLE_ARN')
    jwt_token = get_jwt_token()
    if jwt_token is None:
        logging.error("Failed to obtain JWT token. Aborting upload to S3.")
        return
    role_session_name = 'source_session'
    sts_client = boto3.client('sts', endpoint_url=sts_endpoint_url)
    assumed_role = sts_client.assume_role_with_web_identity(
        RoleArn=role_arn, RoleSessionName=role_session_name, WebIdentityToken=jwt_token
    )
    s3 = boto3.client(
        's3',
        aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
        aws_session_token=assumed_role['Credentials']['SessionToken'],
        endpoint_url=s3_endpoint_url
    )
    with open(file_name, 'rb') as file_data:
        s3.upload_fileobj(file_data, bucket_name, file_name)
    logging.info(f"Uploaded {file_name} to S3 bucket {bucket_name}")

def generate_clients(num_clients):
    """Generates a list of unique client IDs."""
    return [fake.unique.random_int(min=100000, max=999999) for _ in range(num_clients)]

def generate_items_with_categories(num_items):
    """Generates a list of items with unique IDs and categories."""
    items = []
    for _ in range(num_items):
        category = random.choice(list(product_names_by_category.keys()))
        item_name = random.choice(product_names_by_category[category])
        item_id = fake.unique.uuid4()
        items.append((item_id, item_name, category))
    return items

def generate_transactions_with_items(clients, items, num_transactions):
    """Generates transaction data."""
    transactions = []
    item_pool = random.choices(items, k=num_transactions)  # Select with replacement
    for i in range(num_transactions):
        item = item_pool[i]
        item_id = item[0]# Match item_id with browsing logs
        client_id = random.choice(clients)
        transaction_id = fake.uuid4()
        item_description = item[1]
        category = item[2]
        quantity = random.randint(1, 3)
        total_amount = quantity * random.uniform(20.0, 200.0)
        credit_card_number = fake.credit_card_number(card_type=None)
        transaction_date = fake.date_time_this_year()
        transactions.append([client_id, transaction_id, item_id, item_description, category, quantity, total_amount, credit_card_number, transaction_date])
    return pd.DataFrame(transactions, columns=["Client ID", "Transaction ID", "Item ID", "Item Description", "Category", "Quantity", "Total Amount", "Credit Card Number", "Transaction Date"])

def generate_full_browsing_logs(clients, items, num_logs):
    """Generates browsing log data."""
    os_choices = ['Windows', 'MacOS', 'Linux', 'iOS', 'Android']
    browser_choices = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
    logs = []
    item_pool = random.choices(items, k=num_logs)  # Select with replacement for browsing
    for i in range(num_logs):
        item = item_pool[i]
        item_id = item[0]  # Ensure matching item_id with transactions
        client_id = random.choice(clients)
        session_id = fake.uuid4()
        item_description = item[1]
        category = item[2]
        ip_address = fake.ipv4()
        os = random.choice(os_choices)
        browser = random.choice(browser_choices)
        view_date = fake.date_time_this_year()
        day_name = calendar.day_name[view_date.weekday()]
        view_duration = random.randint(5, 100)
        preferred_client = random.choice(["True", "False"])
        price = round(random.uniform(10.0, 100.0), 2)
        tz = "UTC"
        logs.append([
            ip_address, view_date.strftime('%Y-%m-%d %H:%M:%S'), tz, "GET",
            "Item", item_id, 200 if view_duration < 300 else 500, browser, os,
            client_id, day_name, price, category, item_description, preferred_client,
            view_date.strftime('%Y-%m-%d')
        ])
    return pd.DataFrame(logs, columns=[
        "ip", "ts", "tz", "verb", "resource_type", "resource_fk", "response",
        "browser", "os", "customer", "d_day_name", "i_current_price", "i_category",
        "i_description", "c_preferred_cust_flag", "ds"
    ])

def main():
    parser = argparse.ArgumentParser(description='Generate retail datasets with realistic trends.')
    parser.add_argument('num_clients', type=int, help='Number of clients to generate')
    parser.add_argument('num_items', type=int, help='Number of items to generate')
    parser.add_argument('num_transactions', type=int, help='Number of transactions to generate')
    parser.add_argument('num_logs', type=int, help='Number of browsing logs to generate')
    parser.add_argument('--output_parquet', action='store_true', help='Output in Parquet format instead of CSV')
    args = parser.parse_args()

    clients = generate_clients(args.num_clients)
    items = generate_items_with_categories(args.num_items)
    transaction_data = generate_transactions_with_items(clients, items, args.num_transactions)
    browsing_data = generate_full_browsing_logs(clients, items, args.num_logs)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if args.output_parquet:
        transaction_file = f'transaction_data_{timestamp}.parquet'
        browsing_file = f'browsing_data_{timestamp}.parquet'
        transaction_data.to_parquet(transaction_file, index=False)
        browsing_data.to_parquet(browsing_file, index=False)
        print(f'Data generated and saved to Parquet files: {transaction_file} and {browsing_file}')
    else:
        transaction_file = f'transaction_data_{timestamp}.csv'
        browsing_file = f'browsing_data_{timestamp}.csv'
        transaction_data.to_csv(transaction_file, index=False)
        browsing_data.to_csv(browsing_file, index=False)
        print(f'Data generated and saved to CSV files: {transaction_file} and {browsing_file}')

    upload_to_s3(transaction_file, os.getenv('S3_BUCKET_NAME'))
    upload_to_s3(browsing_file, os.getenv('S3_BUCKET_NAME'))

if __name__ == "__main__":
    main()
