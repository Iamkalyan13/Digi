# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 🌊 Data Generator — Simulated Live Feed
# MAGIC ## 🍕 FoodRush India | Food Delivery Orders Pipeline
# MAGIC ---
# MAGIC ### What this notebook does:
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Define the landing zone path |
# MAGIC | 2 | Define a pool of realistic FoodRush India order records |
# MAGIC | 3 | Every **10 seconds**, pick 5 random records and write a new CSV file |
# MAGIC | 4 | Each file is uniquely named using a timestamp |
# MAGIC | 5 | Run indefinitely until you **stop the cell manually** |
# MAGIC
# MAGIC > 🔑 **Purpose:** This notebook simulates a real-time order feed arriving in the landing zone.
# MAGIC > Run this notebook **first**, then run the Bronze streaming notebook alongside it.
# MAGIC
# MAGIC > ⚠️ **How to stop:** Click the **Interrupt** (■) button on the running cell. The stream will stop cleanly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Step 1: Define Landing Zone Path

# COMMAND ----------

raw_landing_path = "dbfs:/FileStore/foodrush/landing/orders/"

# Create the landing zone folder if it doesn't exist
dbutils.fs.mkdirs(raw_landing_path)

print("=" * 55)
print("   FoodRush India — Data Generator")
print("=" * 55)
print(f"  📂 Landing Zone : {raw_landing_path}")
print(f"  ⏱️  Interval     : 10 seconds")
print(f"  📦 Batch size   : 5 records per file")
print("=" * 55)
print()
print("  ▶️  Run the cell below to start generating files.")
print("  ■   Click Interrupt to stop.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Step 2: Full Order Record Pool
# MAGIC
# MAGIC A pool of 40 realistic FoodRush India orders.
# MAGIC Every 10 seconds, 5 are randomly picked and written as a new CSV file.

# COMMAND ----------

import time
import random
from datetime import datetime, timedelta

# CSV header — must match Bronze schema exactly
CSV_HEADER = "order_id,customer_id,restaurant_name,cuisine_type,city,order_date,order_time,food_item,quantity,item_price,delivery_fee,payment_mode,delivery_status,delivery_minutes,customer_rating"

# Pool of 40 order templates
# Fields: restaurant_name, cuisine_type, city, food_item, quantity, item_price, delivery_fee, payment_mode, delivery_status, delivery_minutes, customer_rating
ORDER_POOL = [
    ("Green Leaf Cafe",    "Healthy",       "Bangalore", "Veg Bowl",              1, 180.00, 25.00, "UPI",    "Delivered",  24, 4.6),
    ("Hyderabad Spice",    "Biryani",       "Hyderabad", "Mutton Biryani",        1, 330.00, 40.00, "Card",   "Delivered",  34, 4.7),
    ("Chennai Tiffin",     "South Indian",  "Chennai",   "Pongal",                2,  85.00, 20.00, "UPI",    "Delivered",  18, 4.8),
    ("Pune Pizza Co",      "Pizza",         "Pune",      "Farm Fresh Pizza",      1, 290.00, 45.00, "Wallet", "Delivered",  41, 4.2),
    ("Mumbai Meals",       "North Indian",  "Mumbai",    "Paneer Thali",          2, 210.00, 35.00, "Card",   "Delivered",  39, 4.4),
    ("Coastal Catch",      "Seafood",       "Chennai",   "Fish Curry Rice",       1, 260.00, 30.00, "UPI",    "Delivered",  46, 4.1),
    ("Street Bowl",        "Street Food",   "Hyderabad", "Pav Bhaji",             2, 120.00, 20.00, "Cash",   "Delivered",  27, 4.5),
    ("Urban Burger",       "Burger",        "Bangalore", "Chicken Burger",        1, 220.00, 30.00, "UPI",    "Delivered",  31, 4.3),
    ("Royal Curry",        "North Indian",  "Pune",      "Kadai Paneer",           1, 190.00, 25.00, "Card",   "Delivered",  37, 4.6),
    ("Wok Express",        "Chinese",       "Mumbai",    "Hakka Noodles",         2, 145.00, 25.00, "Wallet", "Delivered",  29, 4.5),
    ("Morning Bites",      "Breakfast",     "Bangalore", "Masala Dosa",           2, 110.00, 20.00, "UPI",    "Delivered",  21, 4.9),
    ("Biryani Junction",   "Biryani",       "Hyderabad", "Veg Biryani",            2, 180.00, 30.00, "Card",   "Delivered",  32, 4.4),
    ("Tiffin Town",        "South Indian",  "Chennai",   "Vada Sambar",            3,  65.00, 15.00, "Cash",   "Delivered",  16, 4.7),
    ("Bombay Dabba",       "North Indian",  "Mumbai",    "Dal Tadka Rice",         1, 155.00, 25.00, "UPI",    "Delivered",  44, 4.0),
    ("Pune Pasta Hub",     "Italian",       "Pune",      "White Sauce Pasta",      1, 240.00, 35.00, "Card",   "Cancelled",   0, 0.0),
    ("Chennai Curry",      "South Indian",  "Chennai",   "Lemon Rice",             2,  75.00, 18.00, "UPI",    "Delivered",  23, 4.6),
    ("Mumbai Biryani",     "Biryani",       "Mumbai",    "Chicken Biryani",        2, 270.00, 40.00, "Wallet", "Delivered",  43, 4.2),
    ("Burger Street",      "Burger",        "Bangalore", "Veg Cheese Burger",      2, 165.00, 25.00, "UPI",    "Delivered",  36, 4.4),
    ("Deccan Kitchen",     "North Indian",  "Hyderabad", "Rajma Rice",             2, 135.00, 22.00, "Card",   "Delivered",  33, 4.5),
    ("Dragon Wok",         "Chinese",       "Pune",      "Schezwan Fried Rice",    1, 170.00, 28.00, "UPI",    "Delivered",  26, 4.7),
    ("Kebab Nation",       "Mughlai",       "Mumbai",    "Chicken Kebab",          2, 230.00, 35.00, "Card",   "Delivered",  38, 4.6),
    ("Wrap Works",         "Fast Food",     "Bangalore", "Veggie Wrap",            1, 125.00, 20.00, "UPI",    "Delivered",  19, 4.5),
    ("Telangana Kitchen",  "Regional",      "Hyderabad", "Chicken Curry Rice",     1, 230.00, 30.00, "Wallet", "Delivered",  47, 4.1),
    ("Madras Mini Meals",  "South Indian",  "Chennai",   "Mini Meals",             1, 150.00, 20.00, "Cash",   "Delivered",  28, 4.8),
    ("Cafe Corner",        "Beverages",     "Pune",      "Cold Coffee",            2, 120.00, 18.00, "UPI",    "Delivered",  17, 4.6),
    ("Chaat Junction",     "Street Food",   "Hyderabad", "Dahi Puri",              1,  90.00, 15.00, "UPI",    "Delivered",  22, 4.9),
    ("Malabar Kitchen",    "Kerala",        "Bangalore", "Appam Stew",             1, 210.00, 28.00, "Card",   "Delivered",  35, 4.7),
    ("Punjabi Rasoi",      "North Indian",  "Mumbai",    "Chole Kulcha",           2, 145.00, 25.00, "Cash",   "Delivered",  30, 4.3),
    ("Dosa Depot",         "South Indian",  "Chennai",   "Cheese Dosa",             1, 130.00, 20.00, "UPI",    "Delivered",  25, 4.5),
    ("Pasta Avenue",       "Italian",       "Pune",      "Arrabbiata Pasta",       1, 220.00, 35.00, "Card",   "Cancelled",   0, 0.0),
    ("Andhra Spice",       "Regional",      "Bangalore", "Gongura Rice",            2, 160.00, 25.00, "UPI",    "Delivered",  40, 4.4),
    ("Royal Thali",        "North Indian",  "Hyderabad", "Special Thali",           1, 280.00, 35.00, "Card",   "Delivered",  42, 4.6),
    ("Chennai Express",    "South Indian",  "Chennai",   "Idiyappam",               2,  95.00, 18.00, "Wallet", "Delivered",  20, 4.9),
    ("Mumbai Frankie",     "Fast Food",     "Mumbai",    "Chicken Frankie",         2, 135.00, 22.00, "UPI",    "Delivered",  24, 4.5),
    ("Healthy Harvest",    "Salad",         "Pune",      "Paneer Salad",             1, 230.00, 30.00, "Card",   "Delivered",  34, 4.0),
    ("Sweet Spot",         "Desserts",      "Hyderabad", "Rasmalai",                2, 110.00, 18.00, "UPI",    "Delivered",  14, 4.9),
    ("Bangalore Tandoor",  "North Indian",  "Bangalore", "Chicken Tikka",            1, 260.00, 35.00, "Cash",   "Delivered",  49, 4.2),
    ("Rice Bowl Co",       "Asian",         "Chennai",   "Teriyaki Rice Bowl",       1, 200.00, 25.00, "UPI",    "Delivered",  37, 4.3),
    ("Pune Snack House",   "Fast Food",     "Pune",      "Cheese Sandwich",           2, 105.00, 18.00, "Wallet", "Delivered",  23, 4.6),
    ("Hyderabad Grill",    "Mughlai",       "Hyderabad", "Grilled Chicken",          1, 310.00, 40.00, "Card",   "Cancelled",   0, 0.0),
]

print(f"✅ Order pool ready — {len(ORDER_POOL)} order templates loaded")
print("   Every 10 seconds, 5 random orders will be written as a new CSV file.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Step 3: Start the Generator
# MAGIC
# MAGIC > ▶️ **Run this cell** — it will keep writing files every 10 seconds.
# MAGIC > ■ **Click Interrupt** on this cell when you want to stop.

# COMMAND ----------

# Counters
batch_number  = 1
total_records = 0

# Starting order_id counter — each run generates unique IDs
order_id_counter = 1000

print("🚀 Generator started!")
print("-" * 55)

try:
    while True:
        # Pick 5 random orders from the pool
        selected = random.sample(ORDER_POOL, 5)

        # Current timestamp for order_date and order_time
        now = datetime.now()

        # Build CSV rows
        rows = [CSV_HEADER]
        for template in selected:
            (restaurant_name, cuisine_type, city, food_item,
             quantity, item_price, delivery_fee, payment_mode,
             delivery_status, delivery_minutes, customer_rating) = template

            order_id_counter += 1
            cust_id    = f"CUST{random.randint(200, 999)}"
            order_date = now.strftime("%Y-%m-%d")
            order_time = now.strftime("%H:%M:%S")

            # Cancelled orders have empty delivery_minutes and customer_rating
            if delivery_status == "Cancelled":
                row = (
                    f"ORD{order_id_counter},{cust_id},{restaurant_name},{cuisine_type},"
                    f"{city},{order_date},{order_time},{food_item},{quantity},"
                    f"{item_price:.2f},{delivery_fee:.2f},{payment_mode},{delivery_status},,"
                )
            else:
                row = (
                    f"ORD{order_id_counter},{cust_id},{restaurant_name},{cuisine_type},"
                    f"{city},{order_date},{order_time},{food_item},{quantity},"
                    f"{item_price:.2f},{delivery_fee:.2f},{payment_mode},{delivery_status},"
                    f"{delivery_minutes},{customer_rating}"
                )
            rows.append(row)

        # File name uses timestamp — guarantees uniqueness every batch
        file_timestamp = now.strftime("%Y%m%d_%H%M%S")
        filename       = f"orders_batch_{file_timestamp}.csv"
        file_path      = raw_landing_path + filename
        csv_content    = "\n".join(rows)

        # Write to DBFS landing zone
        dbutils.fs.put(file_path, csv_content, overwrite=True)

        total_records += 5
        print(f"  ✅ Batch {batch_number:>3}  |  {filename}  |  5 records written  |  Total so far: {total_records}")

        batch_number += 1

        # Wait 10 seconds before next batch
        time.sleep(10)

except KeyboardInterrupt:
    print()
    print("-" * 55)
    print(f"🛑 Generator stopped by user.")
    print(f"   Total batches written : {batch_number - 1}")
    print(f"   Total records written : {total_records}")
    print(f"   Files in landing zone : dbfs:/FileStore/foodrush/landing/orders/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Step 4: Verify Files in Landing Zone (Run after stopping)

# COMMAND ----------

print("📂 Files currently in landing zone:")
files = dbutils.fs.ls(raw_landing_path)
print(f"   Total files: {len(files)}")
print()
for f in sorted(files, key=lambda x: x.name):
    print(f"   📄 {f.name}  ({f.size} bytes)")
