import os
import csv
import random
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

def generate_ecommerce_data(output_dir="data_source/raw_data", num_customers=150, num_products=40, num_orders=600):
    os.makedirs(output_dir, exist_ok=True)
    
    cities_states = [
        ("Mumbai", "Maharashtra"),
        ("Bengaluru", "Karnataka"),
        ("Delhi", "Delhi"),
        ("Hyderabad", "Telangana"),
        ("Pune", "Maharashtra"),
        ("Chennai", "Tamil Nadu"),
        ("Kolkata", "West Bengal"),
        ("Ahmedabad", "Gujarat"),
        ("Jaipur", "Rajasthan"),
        ("Chandigarh", "Punjab")
    ]
    
    first_names = ["Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Reyansh", "Muhammad", "Sai", "Arnav", "Ayaan",
                   "Diya", "Saanvi", "Ananya", "Aadhya", "Pari", "Isha", "Navya", "Riya", "Myra", "Sneha",
                   "Rohan", "Vikram", "Neha", "Pooja", "Rahul", "Priya", "Amit", "Kavya", "Deepak", "Tanvi"]
    last_names = ["Sharma", "Verma", "Patel", "Mehta", "Gupta", "Singh", "Reddy", "Nair", "Iyer", "Rao",
                  "Joshi", "Deshmukh", "Chopra", "Malhotra", "Bhatia", "Saxena", "Choudhury", "Bose", "Menon", "Kulkarni"]
    
    # 1. Customers
    customers = []
    base_date = datetime(2025, 1, 1)
    for i in range(1, num_customers + 1):
        c_id = f"CUST_{i:04d}"
        fname = random.choice(first_names)
        lname = random.choice(last_names)
        city, state = random.choice(cities_states)
        email = f"{fname.lower()}.{lname.lower()}{random.randint(10,999)}@example.com"
        phone = f"+91-9{random.randint(100000000, 999999999)}"
        signup_offset = random.randint(0, 365)
        signup_date = (base_date + timedelta(days=signup_offset)).strftime("%Y-%m-%d %H:%M:%S")
        customers.append({
            "customer_id": c_id,
            "first_name": fname,
            "last_name": lname,
            "email": email,
            "phone": phone,
            "city": city,
            "state": state,
            "signup_date": signup_date
        })
    
    with open(os.path.join(output_dir, "customers.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(customers[0].keys()))
        writer.writeheader()
        writer.writerows(customers)
    print(f"Generated {len(customers)} customers.")

    # 2. Products
    categories = {
        "Electronics": [
            ("Wireless Noise-Cancelling Headphones", 2999, 1500),
            ("Mechanical Gaming Keyboard", 4499, 2300),
            ("Ergonomic Wireless Mouse", 1299, 650),
            ("Smartwatch Ultra Fitness", 5999, 3200),
            ("Fast USB-C 65W GaN Charger", 1499, 700),
            ("4K HD Webcam with Mic", 3499, 1800),
            ("Portable Bluetooth Speaker 20W", 2199, 1100),
            ("Noise-Free Wireless Earbuds", 1999, 950)
        ],
        "Home & Kitchen": [
            ("Stainless Steel Electric Kettle", 1199, 600),
            ("Cold Press Juicer", 4999, 2600),
            ("Memory Foam Ergonomic Pillow", 1499, 750),
            ("Non-Stick Induction Pan 28cm", 1299, 650),
            ("Aroma Diffuser Ultrasonic", 899, 420),
            ("Smart Air Purifier HEPA H13", 8999, 4800),
            ("Insulated Travel Coffee Mug", 699, 300)
        ],
        "Fashion & Apparel": [
            ("Oversized Heavyweight Cotton Hoodie", 1899, 850),
            ("Slim Fit Stretch Denim Jeans", 2199, 1000),
            ("Classic Oxford Formal Shirt", 1599, 700),
            ("Lightweight Running Shoes", 2999, 1400),
            ("UV Protected Polarized Sunglasses", 1299, 500),
            ("Leather Minimalist Wallet", 899, 350)
        ],
        "Fitness & Sports": [
            ("Non-Slip Natural Rubber Yoga Mat", 1499, 700),
            ("Adjustable Hex Dumbbell Set 20kg", 3999, 2100),
            ("Skipping Rope with Speed Bearings", 499, 180),
            ("Stainless Steel Protein Shaker 750ml", 799, 320),
            ("Resistance Bands Set of 5", 999, 420)
        ],
        "Books & Stationery": [
            ("Designing Data-Intensive Applications", 1499, 900),
            ("Atomic Habits Hardcover", 699, 400),
            ("Minimalist Dotted Bullet Journal", 499, 220),
            ("Luxury Brass Fountain Pen", 1199, 550)
        ]
    }
    
    products = []
    p_counter = 1
    for cat, items in categories.items():
        for name, price, cost in items:
            products.append({
                "product_id": f"PROD_{p_counter:04d}",
                "product_name": name,
                "category": cat,
                "price": price,
                "cost": cost,
                "inventory_stock": random.randint(15, 250)
            })
            p_counter += 1
            
    with open(os.path.join(output_dir, "products.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(products[0].keys()))
        writer.writeheader()
        writer.writerows(products)
    print(f"Generated {len(products)} products.")

    # 3. Orders & Order Items & Payments
    orders = []
    order_items = []
    payments = []
    
    payment_methods = ["Credit Card", "UPI", "Debit Card", "Net Banking", "Cash on Delivery"]
    order_statuses = ["Delivered", "Delivered", "Delivered", "Shipped", "Processing", "Cancelled"]
    
    order_start_date = datetime(2025, 3, 1)
    
    item_counter = 1
    payment_counter = 1
    
    for i in range(1, num_orders + 1):
        order_id = f"ORD_{i:05d}"
        cust = random.choice(customers)
        cust_id = cust["customer_id"]
        
        # Order date
        day_offset = random.randint(0, 300)
        order_dt = order_start_date + timedelta(days=day_offset, hours=random.randint(8, 22), minutes=random.randint(0, 59))
        order_date_str = order_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        status = random.choice(order_statuses)
        pay_method = random.choice(payment_methods)
        
        # Shipping and SLA
        shipping_city = cust["city"]
        shipping_fee = 0 if random.random() > 0.4 else random.choice([49, 99])
        
        # Estimated delivery 3 to 5 days
        est_days = random.randint(3, 5)
        est_delivery_dt = order_dt + timedelta(days=est_days)
        
        if status == "Delivered":
            # 80% on time, 20% delayed
            delay_factor = random.choices([0, 1, 2, 4], weights=[0.8, 0.1, 0.06, 0.04])[0]
            actual_delivery_dt = order_dt + timedelta(days=est_days + delay_factor, hours=random.randint(1, 10))
            actual_del_str = actual_delivery_dt.strftime("%Y-%m-%d %H:%M:%S")
        elif status == "Shipped":
            actual_del_str = ""
        else:
            actual_del_str = ""
            
        # Order items (1 to 4 items per order)
        num_items = random.choices([1, 2, 3, 4], weights=[0.55, 0.28, 0.12, 0.05])[0]
        chosen_products = random.sample(products, num_items)
        
        order_subtotal = 0
        for prod in chosen_products:
            qty = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
            unit_price = prod["price"]
            discount = random.choice([0, 0, 50, 100, 200]) if unit_price > 1000 else 0
            line_total = (unit_price * qty) - discount
            order_subtotal += line_total
            
            order_items.append({
                "order_item_id": f"ITEM_{item_counter:06d}",
                "order_id": order_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
                "discount_amount": discount,
                "line_total": line_total
            })
            item_counter += 1
            
        total_order_value = order_subtotal + shipping_fee
        
        orders.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "order_date": order_date_str,
            "order_status": status,
            "payment_method": pay_method,
            "shipping_city": shipping_city,
            "shipping_fee": shipping_fee,
            "estimated_delivery_date": est_delivery_dt.strftime("%Y-%m-%d"),
            "actual_delivery_date": actual_del_str
        })
        
        # Payment record
        if status != "Cancelled":
            payments.append({
                "payment_id": f"PAY_{payment_counter:05d}",
                "order_id": order_id,
                "payment_date": order_date_str,
                "payment_method": pay_method,
                "amount": total_order_value,
                "payment_status": "Success" if random.random() > 0.03 else "Failed"
            })
            payment_counter += 1
            
    with open(os.path.join(output_dir, "orders.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(orders[0].keys()))
        writer.writeheader()
        writer.writerows(orders)
        
    with open(os.path.join(output_dir, "order_items.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(order_items[0].keys()))
        writer.writeheader()
        writer.writerows(order_items)
        
    with open(os.path.join(output_dir, "payments.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(payments[0].keys()))
        writer.writeheader()
        writer.writerows(payments)
        
    print(f"Generated {len(orders)} orders, {len(order_items)} line items, and {len(payments)} payments.")
    print(f"Data saved to {output_dir}")

if __name__ == "__main__":
    generate_ecommerce_data()
