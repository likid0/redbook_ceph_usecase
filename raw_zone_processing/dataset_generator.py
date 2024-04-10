import csv
import re
import os
import logging
import random
import datetime
from faker import Faker
import boto3
import requests  # Add import statement for requests module

# Initialize logging
logging.basicConfig(level=logging.INFO)

# List of required environment variables
REQUIRED_ENV_VARS = [
    'S3_ENDPOINT_URL',
    'STS_ENDPOINT_URL',
    'SOURCE_ROLE_ARN',
    'OIDC_PROVIDER_URL',
    'OIDC_CLIENT_ID',
    'OIDC_CLIENT_SECRET',
    'OIDC_USERNAME',
    'OIDC_PASSWORD',
    'S3_BUCKET_NAME'
]

def check_env_variables():
    missing_vars = [var for var in REQUIRED_ENV_VARS if var not in os.environ]
    if missing_vars:
        logging.error("The following environment variables are missing:")
        for var in missing_vars:
            logging.error(var)
        return False
    return True

def generate_fake_data(shop_id, date, num_entries):
    # Validate date format
    try:
        parsed_date = datetime.datetime.strptime(date, '%d-%m-%Y')
    except ValueError:
        logging.error("Invalid date format. Please use DD-MM-YYYY format.")
        return

    # Set file name
    file_name = f"{shop_id}_{date.replace('-', '_')}.csv"

    # Create Faker instance
    fake = Faker()

    # Define fake data
    product_categories = ["Clothing", "Accessories", "Footwear", "Electronics", "Jewelry"]
    product_names_by_category = {
        "Clothing": [
            "T-shirt", "Jeans", "Dress", "Jacket", "Sweater", "Skirt", "Scarf", "Gloves", "Socks", "Hat", "Coat", "Blouse", "Pants", "Hoodie", "Pajamas"
        ],
        "Accessories": [
            "Belt", "Bag", "Watch", "Hat", "Scarf", "Gloves", "Socks", "Tie", "Wallet", "Backpack", "Bracelet", "Earrings", "Necklace", "Ring", "Briefcase"
        ],
        "Footwear": [
            "Sneakers", "Shoes", "Boots", "Sandals", "Slippers"
        ],
        "Electronics": [
            "Phone", "Laptop", "Tablet", "Smartwatch", "Headphones", "Speaker", "Camera", "Charger", "Power Bank", "Mouse", "Keyboard", "Monitor", "TV"
        ],
        "Jewelry": [
            "Ring", "Necklace", "Earrings", "Bracelet", "Pendant", "Brooch", "Chain", "Cufflinks", "Anklet", "Charm", "Choker", "Pin", "Tiara", "Watch"
        ]
    }

    # Generate fake data
    fake_data = []
    for _ in range(num_entries):
        invoice_no = random.randint(10000, 99999)
        stock_code = random.randint(10000, 99999)
        product_category = random.choice(product_categories)
        product_name = random.choice(product_names_by_category[product_category])
        quantity = random.randint(1, 20)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        invoice_date = parsed_date.replace(hour=hour, minute=minute).strftime('%d-%m-%Y %H:%M')
        price = round(random.uniform(1, 100), 2)
        customer_id = random.randint(10000, 99999)
        country = get_country_from_shop_id(shop_id)
        ssn = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
        email = fake.email()
        payment_method = random.choice(["Credit Card", "Cash"])
        
        fake_data.append([invoice_no, stock_code, product_name, quantity, invoice_date, price, customer_id, country, ssn, email, payment_method, product_category])

    # Write to CSV file
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'Price', 'CustomerID', 'Country', 'SSN', 'Email', 'PaymentMethod', 'ProductCategory'])
        writer.writerows(fake_data)

    logging.info(f"Fake data generated and saved to '{file_name}'")

    # Upload to S3
    upload_to_s3(file_name)

def get_country_from_shop_id(shop_id):
    # Define mapping of shop ID to country
    country_mapping = {
        "shop1": "Spain",
        "shop2": "France",
        "shop3": "Germany",
        "shop4": "Italy",
        "shop5": "Netherlands"
    }

    # Look up country based on shop ID
    return country_mapping.get(shop_id, "Unknown")

def upload_to_s3(file_name):
    # Check if all required environment variables are set
    if not check_env_variables():
        logging.error("Missing required environment variables. Aborting upload to S3.")
        return

    # Set S3 configuration
    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    sts_endpoint_url = os.getenv('STS_ENDPOINT_URL')
    role_arn = os.getenv('SOURCE_ROLE_ARN')
    provider_url = os.getenv('OIDC_PROVIDER_URL')
    client_id = os.getenv('OIDC_CLIENT_ID')
    client_secret = os.getenv('OIDC_CLIENT_SECRET')

    # Initialize STS client with custom endpoint
    sts_client = boto3.client('sts', endpoint_url=sts_endpoint_url)

    try:
        # Get JWT token
        provider_url = os.getenv('OIDC_PROVIDER_URL')
        client_id = os.getenv('OIDC_CLIENT_ID')
        client_secret = os.getenv('OIDC_CLIENT_SECRET')
        jwt_token = get_jwt_token(provider_url, client_id, client_secret)
        if jwt_token is None:
            logging.error("Failed to obtain JWT token. Aborting upload to S3.")
            return

        # Assume role
        role_session_name = 'source_session'
        assumed_role = sts_client.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=role_session_name,
            WebIdentityToken=jwt_token
        )

        # Initialize S3 client with temporary credentials
        s3 = boto3.client(
            's3',
            aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role['Credentials']['SessionToken'],
            endpoint_url=s3_endpoint_url
        )

        # Upload file to S3 bucket
        bucket_name = os.getenv('S3_BUCKET_NAME')
        s3.upload_file(file_name, bucket_name, file_name)

        logging.info(f"Uploaded {file_name} to S3 bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Error uploading {file_name} to S3: {e}")

def get_jwt_token(provider_url, client_id, client_secret):
    # Send authentication request to OIDC provider to obtain JWT token
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
        token_data = response.json()
        return token_data['access_token']
    except Exception as e:
        logging.error(f"Error obtaining JWT token: {e}")
        return None

if __name__ == "__main__":
    import argparse

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Generate fake retail CSV dataset and upload to S3.")
    parser.add_argument("shop_id", type=str, help="Shop ID")
    parser.add_argument("date", type=str, help="Date in DD-MM-YYYY format")
    parser.add_argument("num_entries", type=int, help="Number of entries to generate")
    args = parser.parse_args()

    # Generate fake data
    generate_fake_data(args.shop_id, args.date, args.num_entries)

