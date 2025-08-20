import pandas as pd
import numpy as np
import random
from faker import Faker

fake = Faker()
np.random.seed(42)
random.seed(42)

# -------------------------
# 1. Customers
# -------------------------
def generate_customers(n=10000):   # Updated to 10k
    customers = []
    for i in range(1, n+1):
        customers.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "signup_date": fake.date_between(start_date="-3y", end_date="today"),
            "country": fake.country(),
        })
    return pd.DataFrame(customers)

# -------------------------
# 2. Products
# -------------------------
def generate_products(n=20000):   # Updated to 20k
    categories = ["Electronics", "Clothing", "Books", "Home", "Beauty", "Sports", "Toys"]
    products = []
    for i in range(1, n+1):
        products.append({
            "product_id": i,
            "name": fake.word().capitalize() + " " + random.choice(["Pro", "Max", "Lite", "Plus"]),
            "category": random.choice(categories),
            "price": round(random.uniform(5, 500), 2),
            "rating": round(random.uniform(1, 5), 1),
        })
    return pd.DataFrame(products)

# -------------------------
# 3. Orders
# -------------------------
def generate_orders(customers, products, n=100000):   # Updated to 1 lakh
    orders = []
    for i in range(1, n+1):
        cust = random.choice(customers["customer_id"])
        prod = random.choice(products["product_id"])
        qty = random.randint(1, 5)
        discount = round(random.uniform(0, 0.3), 2)
        orders.append({
            "order_id": i,
            "customer_id": cust,
            "product_id": prod,
            "quantity": qty,
            "discount": discount,
            "order_date": fake.date_between(start_date="-2y", end_date="today"),
        })
    return pd.DataFrame(orders)

# -------------------------
# 4. Reviews
# -------------------------
def generate_reviews(customers, products, n=40000):   # Updated to 40k
    reviews = []
    for i in range(1, n+1):
        cust = random.choice(customers["customer_id"])
        prod = random.choice(products["product_id"])
        reviews.append({
            "review_id": i,
            "customer_id": cust,
            "product_id": prod,
            "rating": random.randint(1, 5),
            "review_text": fake.sentence(nb_words=10),
            "review_date": fake.date_between(start_date="-2y", end_date="today"),
        })
    return pd.DataFrame(reviews)

# -------------------------
# Main function
# -------------------------
if __name__ == "__main__":
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders(customers, products)
    reviews = generate_reviews(customers, products)

    # Save CSVs
    customers.to_csv("customers.csv", index=False)
    products.to_csv("products.csv", index=False)
    orders.to_csv("orders.csv", index=False)
    reviews.to_csv("reviews.csv", index=False)

    print("✅ Synthetic data generated: customers.csv (10k), products.csv (20k), orders.csv (100k), reviews.csv (40k)")
