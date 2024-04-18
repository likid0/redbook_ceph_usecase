
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import calendar
import sys

# Initialize Faker
fake = Faker()

product_names_by_category = {
    "Clothing": ["T-shirt", "Jeans", "Dress", "Jacket", "Sweater", "Skirt", "Scarf", "Gloves", "Socks", "Hat", "Coat", "Blouse", "Pants", "Hoodie", "Pajamas"],
    "Accessories": ["Belt", "Bag", "Watch", "Hat", "Scarf", "Gloves", "Socks", "Tie", "Wallet", "Backpack", "Bracelet", "Earrings", "Necklace", "Ring", "Briefcase"],
    "Footwear": ["Sneakers", "Shoes", "Boots", "Sandals", "Slippers"],
    "Electronics": ["Phone", "Laptop", "Tablet", "Smartwatch", "Headphones", "Speaker", "Camera", "Charger", "Power Bank", "Mouse", "Keyboard", "Monitor", "TV"],
    "Jewelry": ["Ring", "Necklace", "Earrings", "Bracelet", "Pendant", "Brooch", "Chain", "Cufflinks", "Anklet", "Charm", "Choker", "Pin", "Tiara", "Watch"]
}

def generate_clients(num_clients):
    return [random.randint(10000, 999999) for _ in range(num_clients)]

def generate_items_with_categories(num_items):
    items = []
    for _ in range(num_items):
        category = random.choice(list(product_names_by_category.keys()))
        item_name = random.choice(product_names_by_category[category])
        item_id = fake.unique.uuid4()
        items.append((item_id, item_name, category))
    return items

def generate_transactions_with_items(clients, items, num_transactions):
    transactions = []
    for _ in range(num_transactions):
        client_id = random.choice(clients)
        transaction_id = fake.uuid4()
        item_id, item_description, category = random.choice(items)
        quantity = random.randint(1, 5)
        transaction_date = fake.date_time_this_year()
        total_amount = quantity * random.uniform(5.0, 100.0)
        credit_card_number = fake.credit_card_number(card_type=None)
        transactions.append([client_id, transaction_id, item_id, item_description, category, quantity, transaction_date, total_amount, credit_card_number])
    return pd.DataFrame(transactions, columns=["Client ID", "Transaction ID", "Item ID", "Item Description", "Category", "Quantity", "Transaction Date", "Total Amount", "Credit Card Number"])

def generate_full_browsing_logs(clients, items, num_logs):
    os_choices = ['Windows', 'MacOS', 'Linux', 'iOS', 'Android']
    browser_choices = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
    logs = []
    for _ in range(num_logs):
        client_id = random.choice(clients)
        session_id = fake.uuid4()
        item_id, item_description, category = random.choice(items)
        ip_address = fake.ipv4()
        os = random.choice(os_choices)
        browser = random.choice(browser_choices)
        view_date = fake.date_time_this_year()
        day_name = calendar.day_name[view_date.weekday()]
        view_duration = random.randint(5, 600)
        preferred_client = random.choice(["True", "False"])
        price = round(random.uniform(10.0, 1000.0), 2)
        tz = "UTC"
        
        logs.append([
            ip_address,
            view_date.strftime('%Y-%m-%d %H:%M:%S'),
            tz,
            "GET",
            "Item",
            session_id,
            200 if view_duration < 300 else 500,
            browser,
            os,
            client_id,
            day_name,
            price,
            category,
            item_description,
            preferred_client,
            view_date.strftime('%Y-%m-%d')
        ])
    return pd.DataFrame(logs, columns=[
        "ip", "ts", "tz", "verb", "resource_type", "resource_fk", "response", "browser", 
        "os", "customer", "d_day_name", "i_current_price", "i_category", "i_description", "c_preferred_cust_flag", "ds"
    ])

def main(num_clients, num_items, num_transactions, num_logs):
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    clients = generate_clients(num_clients)
    items = generate_items_with_categories(num_items)
    transaction_data = generate_transactions_with_items(clients, items, num_transactions)
    browsing_data = generate_full_browsing_logs(clients, items, num_logs)
    
    transaction_data.to_csv(f'transaction_data_{timestamp}.csv', index=False)
    browsing_data.to_csv(f'browsing_data_{timestamp}.csv', index=False)
    print(f"Data generated and saved to CSV files: transaction_data_{timestamp}.csv and browsing_data_{timestamp}.csv")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python script.py <num_clients> <num_items> <num_transactions> <num_logs>")
    else:
        num_clients = int(sys.argv[1])
        num_items = int(sys.argv[2])
        num_transactions = int(sys.argv[3])
        num_logs = int(sys.argv[4])
        main(num_clients, num_items, num_transactions, num_logs)
