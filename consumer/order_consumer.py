from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="order-stats-group",
)

total_orders = 0
total_revenue = 0.0
category_counts = {
    "electronics": 0,
    "clothing": 0,
    "books": 0,
    "home": 0,
    "sports": 0,
}

print("Waiting for messages...")

for msg in consumer:
    line = msg.value.decode("utf-8")
    parts = line.split(",")
    if len(parts) != 7:
        continue

    _, _, _, category, price_str, qty_str, _ = parts

    try:
        price = float(price_str)
        qty = int(qty_str)
    except ValueError:
        continue

    line_total = price * qty
    total_orders += 1
    total_revenue += line_total

    if category in category_counts:
        category_counts[category] += 1

    if total_orders % 100 == 0:
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0.0
        top_cat = max(category_counts.items(), key=lambda x: x[1])[0]

        print("\n--- Stats after", total_orders, "orders ---")
        print("Total revenue:", round(total_revenue, 2))
        print("Average order value:", round(avg_order_value, 2))
        for cat, cnt in category_counts.items():
            print(f"{cat}: {cnt} orders")
        print("Top category so far:", top_cat)

