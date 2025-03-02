from kafka import KafkaProducer
import random
from faker import Faker
import time
import json

# Initialize Faker
fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['158.220.124.0:9094', '158.220.124.0:9093'],  # List of brokers
    retries=5,    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON
    key_serializer=lambda k: str(k).encode('utf-8')  # Serialize keys
)
# Constants
NUM_USERS = 5000
NUM_RESTAURANTS = 10
NUM_MENU_ITEMS = 50
NUM_ORDERS = 4000
NUM_DELIVERIES = 4000

# Generate Users
def generate_users(id):
    user = {
        "user_id": id,
        "username": fake.user_name(),
        "email": fake.email(),
        "password_hash": fake.sha256(),
        "phone_number": fake.phone_number(),
        "address": fake.address(),
        "registration_date": fake.date_time().isoformat(),
        "status": random.choice(['active', 'suspended']),
    }
    print(f"Produced {user} user")
    try:
        # Send the message
                producer.send('users_data', value=user)  # No need to encode manually
                producer.flush()  # Ensure all messages are sent
                print(f"Message sent to topic '{'users_data'}': {user}")
    except Exception as e:
                print(f"Failed to produce message: {e}")
    return user  # Returning user object

# Generate Restaurants
def generate_restaurants(id):
    restaurant = {
        "restaurant_id": id,
        "name": fake.company(),
        "address": fake.address(),
        "phone_number": fake.phone_number(),
        "cuisine": random.choice(['Italian', 'Chinese', 'Indian', 'Mexican', 'American']),
        "rating": round(random.uniform(1, 5), 1),
    }
    print(f"Produced {restaurant} restaurant")
    try:
        # Send the message
                producer.send('restaurant_data', value=restaurant)  # No need to encode manually
                producer.flush()  # Ensure all messages are sent
                print(f"Message sent to topic '{'restaurant_data'}': {restaurant}")
    except Exception as e:
                print(f"Failed to produce message: {e}")
    return restaurant  # Returning restaurant object

# Generate Menu Items
def generate_menu_items(restaurant_id):
    menu_items = []
    for i in range(NUM_MENU_ITEMS // NUM_RESTAURANTS):
        menu_item = {
            "menu_item_id": fake.unique.random_int(min=1, max=100000),
            "restaurant_id": restaurant_id,
            "name": fake.word() + " " + fake.word(),
            "description": fake.sentence(),
            "price": round(random.uniform(5, 50), 2),
            "availability": random.choice([True, False]),
        }
        print(f"Produced {menu_item}")
        menu_items.append(menu_item)
        try:
            # Send the message
                    producer.send('menu_items', value=menu_item)  # No need to encode manually
                    producer.flush()  # Ensure all messages are sent
                    print(f"Message sent to topic '{'menu_items'}': {menu_item}")
        except Exception as e:
                    print(f"Failed to produce message: {e}")

    return menu_items

# Generate Orders
def generate_orders(user_id):
    orders = []
    for i in range(random.randint(1, 5)):  # Each user can place 1 to 5 orders
        order = {
            "order_id": fake.unique.random_int(min=1, max=100000),
            "user_id": user_id,
            "restaurant_id": random.randint(1, NUM_RESTAURANTS),
            "order_date": fake.date_time().isoformat(),
            "total_amount": round(random.uniform(10, 200), 2),
            "status": random.choice(['placed', 'in delivery', 'delivered', 'canceled']),
        }
        print(f"Produced {order}")
        orders.append(order)
        try:
            # Send the message
            producer.send('orders', value=order)  # No need to encode manually
            producer.flush()  # Ensure all messages are sent
            print(f"Message sent to topic '{'order_items'}': {order}")
        except Exception as e:
                    print(f"Failed to produce message: {e}")

    return orders

# Generate Order Items
def generate_order_items(order_id):
    order_items = []
    for i in range(random.randint(1, 3)):  # Each order can have 1 to 3 items
        order_item = {
            "order_item_id": fake.unique.random_int(min=1, max=100000),
            "order_id": order_id,
            "menu_item_id": random.randint(1, NUM_MENU_ITEMS),  # Ensure this links to a menu item
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(5, 50), 2),
        }
        print(f"Produced {order_item}")
        order_items.append(order_item)
        try:
            # Send the message
            producer.send('order_items', value=order_item)  # No need to encode manually
            producer.flush()  # Ensure all messages are sent
            print(f"Message sent to topic '{'order_items'}': {order_item}")
        except Exception as e:
                    print(f"Failed to produce message: {e}")

    return order_items

# Generate Delivery Details
def generate_delivery_details(order_id):
    delivery_detail = {
        "delivery_id": fake.unique.random_int(min=1, max=100000),
        "order_id": order_id,
        "delivery_address": fake.address(),
        "delivery_person": fake.name(),
        "contact_number": fake.phone_number(),
        "delivery_status": random.choice(['assigned', 'on the way', 'delivered']),
        "delivery_time": fake.date_time().isoformat(),
    }
    print(f"Produced {delivery_detail}")
    try:
        # Send the message
        producer.send('delivery_detail', value=delivery_detail)  # No need to encode manually
        producer.flush()  # Ensure all messages are sent
        print(f"Message sent to topic '{'delivery_detail'}': {delivery_detail}")
    except Exception as e:
                print(f"Failed to produce message: {e}")
    return delivery_detail

# Main Function to Generate Data
if __name__ == "__main__":
    i = 1
    while True:
        # Generate Users
        user = generate_users(i)
        
        # Generate Restaurants
        if i <= NUM_RESTAURANTS:
            restaurant = generate_restaurants(i)
            menu_items = generate_menu_items(i)
        
        # Generate Orders and Related Items
        orders = generate_orders(i)
        for order in orders:
            order_items = generate_order_items(order["order_id"])
            delivery_detail = generate_delivery_details(order["order_id"])
        
        i += 1  # Properly increment i
        time.sleep(1)
