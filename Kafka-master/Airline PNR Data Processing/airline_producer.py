from kafka import KafkaProducer
import json
import requests
import time
from faker import Faker
import random

# Initialize the producer
producer = KafkaProducer(
    bootstrap_servers=['158.220.124.0:9094', '158.220.124.0:9095'],  # List of brokers
    retries=5,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON
    key_serializer=lambda k: str(k).encode('utf-8')  # Serialize keys
)

fake = Faker()

# Define constants
BOOKING_STATUSES = ['Confirmed', 'Cancelled', 'Pending']
PAYMENT_METHODS = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']
AIRPORT_CODES = ['JFK', 'LAX', 'ORD', 'LHR', 'DXB', 'SYD', 'HND', 'SFO']


# Function to corrupt data randomly based on the corruption type
def corrupt_data(field, corruption_type):
    if corruption_type == "email":
        # Corrupt the email by removing '@' or domain
        corrupt_options = [
            field.replace('@', ''),  # Remove '@'
            field.split('@')[0],  # Keep only the username part
            field + "@",  # Incomplete email
            "randomemail.com"  # No '@'
        ]
        return random.choice(corrupt_options)

    elif corruption_type == "name":
        # Add random numbers or special characters to name
        return ''.join([char if random.random() > 0.2 else str(random.randint(0, 9)) for char in field])

    elif corruption_type == "phone_number":
        # Add special characters to phone number
        return ''.join([char if char.isdigit() and random.random() > 0.3 else random.choice("!@#$%^&*") for char in field])

    elif corruption_type == "passport_number":
        # Corrupt passport number by adding special characters or making it too short/long
        return field[:random.randint(1, len(field))] + random.choice("!@#$%^&*")

    elif corruption_type == "flight_number":
        # Corrupt flight number by adding special characters or numbers
        return ''.join([char if char.isalnum() and random.random() > 0.3 else random.choice("!@#$%^&*") for char in field])

    elif corruption_type == "airport_code":
        # Corrupt the airport code by changing the characters
        return ''.join([char if random.random() > 0.5 else random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for char in field])

    return field


# Function to generate master record (Customer Personal Details) with possible data corruption
def create_customer(customer_id):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    passport_number = fake.passport_number()

    # Randomly corrupt some fields
    if random.random() < 0.2:  # 20% chance to corrupt email
        email = corrupt_data(email, "email")

    if random.random() < 0.15:  # 15% chance to corrupt first name
        first_name = corrupt_data(first_name, "name")

    if random.random() < 0.15:  # 15% chance to corrupt phone number
        phone_number = corrupt_data(phone_number, "phone_number")

    if random.random() < 0.1:  # 10% chance to corrupt passport number
        passport_number = corrupt_data(passport_number, "passport_number")

    return {
        'customer_id': customer_id,
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        'phone_number': phone_number,
        'passport_number': passport_number,
        'nationality': fake.country()
    }


# Function to generate slave record (PNR Details) with possible data corruption
def create_pnr(customer_id, pnr_id, passport_number):
    departure_airport = random.choice(AIRPORT_CODES)
    arrival_airport = random.choice([code for code in AIRPORT_CODES if code != departure_airport])

    departure_date = fake.date_time_this_year()
    arrival_date = fake.date_time_between(start_date=departure_date, end_date="+2d")
    flight_number = fake.bothify(text='??###')

    # Randomly corrupt some fields
    if random.random() < 0.15:  # 15% chance to corrupt flight number
        flight_number = corrupt_data(flight_number, "flight_number")

    if random.random() < 0.1:  # 10% chance to corrupt departure airport
        departure_airport = corrupt_data(departure_airport, "airport_code")

    if random.random() < 0.1:  # 10% chance to corrupt arrival airport
        arrival_airport = corrupt_data(arrival_airport, "airport_code")

    return {
        'pnr_id': pnr_id,
        'customer_id': customer_id,  # Foreign key from master (customer)
        'flight_number': flight_number,  # Possibly corrupted flight number
        'departure_airport': departure_airport,
        'arrival_airport': arrival_airport,
        'departure_date': departure_date.isoformat(),
        'arrival_date': arrival_date.isoformat(),
        'ticket_price': round(random.uniform(100.0, 1500.0), 2),
        'seat_number': f"{random.randint(1, 30)}{random.choice('ABCDEF')}",  # Random seat (e.g., "12A")
        'booking_status': random.choice(BOOKING_STATUSES),
        'payment_method': random.choice(PAYMENT_METHODS),
        'passport_number': passport_number  # Foreign key, possibly corrupted from customer
    }


# Generate master (customers) and slave (PNR records) records indefinitely
def generate_data(producer):
    customer_id = 1  # Initialize customer ID
    try:
        while True:
            # Create master record (customer)
            customer = create_customer(customer_id)

            # Create slave records (PNRs) for the customer
            num_pnrs = random.randint(1, 3)  # Random number of PNRs for each customer
            for pnr_id in range(1, num_pnrs + 1):
                pnr = create_pnr(customer_id, f'{customer_id}-{pnr_id}', customer['passport_number'])
                producer.send('pnr_data', value=pnr)  # No need to encode manually
                producer.flush()  # Ensure all messages are sent
                print(f"Message sent to topic 'pnr_data': {pnr}")
                # Send the message to Kafka
            try:
                producer.send('customers_data', value=customer)  # No need to encode manually
                producer.flush()  # Ensure all messages are sent
                print(f"Message sent to topic 'customers_data': {customer}")

                producer.send('pnr_data', value=pnr)  # No need to encode manually
                producer.flush()  # Ensure all messages are sent
                print(f"Message sent to topic 'pnr_data': {pnr}")
            except Exception as e:
                print(f"Failed to produce message: {e}")

            # Increment customer ID
            customer_id += 1
            print('\n')  # Print a new line for readability

            # Wait for 1 second before generating the next record
            time.sleep(1)

    except KeyboardInterrupt:
        # When the user stops the infinite loop by pressing Ctrl+C
        print("\nData generation stopped by user.")
        return customer, pnr


if __name__ == "__main__":
    generate_data(producer)
