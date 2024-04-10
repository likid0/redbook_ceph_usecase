import csv
import re
import os
import logging
import random
import datetime
from faker import Faker

# Initialize logging
logging.basicConfig(level=logging.INFO)

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

if __name__ == "__main__":
    import argparse

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Generate fake retail CSV dataset.")
    parser.add_argument("shop_id", type=str, help="Shop ID")
    parser.add_argument("date", type=str, help="Date in DD-MM-YYYY format")
    parser.add_argument("num_entries", type=int, help="Number of entries to generate")
    args = parser.parse_args()

    # Generate fake data
    generate_fake_data(args.shop_id, args.date, args.num_entries)

