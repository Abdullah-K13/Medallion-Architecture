from kafka import KafkaProducer
import json
import requests
import time
from faker import Faker
import random

# Initialize the producer
producer = KafkaProducer(
    bootstrap_servers=['158.220.124.0:9094', '158.220.124.0:9093'],  # List of brokers
    retries=5,    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON
    key_serializer=lambda k: str(k).encode('utf-8')  # Serialize keys
)

fake = Faker()

# Define constants
BOOKING_STATUSES = ['Confirmed', 'Cancelled', 'Pending']
PAYMENT_METHODS = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']
AIRPORT_CODES = ['JFK', 'LAX', 'ORD', 'LHR', 'DXB', 'SYD', 'HND', 'SFO']

# Function to generate master record (Customer Personal Details)
def create_customer(customer_id):
    return {
        'customer_id': customer_id,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'passport_number': fake.passport_number(),
        'nationality': fake.country()
    }

# Function to generate slave record (PNR Details)
def create_pnr(customer_id, pnr_id, passport_number):
    departure_airport = random.choice(AIRPORT_CODES)
    arrival_airport = random.choice([code for code in AIRPORT_CODES if code != departure_airport])
    
    departure_date = fake.date_time_this_year()
    arrival_date = fake.date_time_between(start_date=departure_date, end_date="+2d")

    return {
        'pnr_id': pnr_id,
        'customer_id': customer_id,  # Foreign key from master (customer)
        'flight_number': fake.bothify(text='??###'),  # Random flight number (e.g., "AA123")
        'departure_airport': departure_airport,
        'arrival_airport': arrival_airport,
        'departure_date': departure_date.isoformat(),
        'arrival_date': arrival_date.isoformat(),
        'ticket_price': round(random.uniform(100.0, 1500.0), 2),
        'seat_number': f"{random.randint(1, 30)}{random.choice('ABCDEF')}",  # Random seat (e.g., "12A")
        'booking_status': random.choice(BOOKING_STATUSES),
        'payment_method': random.choice(PAYMENT_METHODS),
        'passport_number': passport_number  # Include passport number in PNR
    }

# Generate master (customers) and slave (PNR records) records indefinitely
def generate_data(producer):
    
    customer_id = 1

    try:
        while True:
            # Create master record (customer)
            customer = create_customer(customer_id)

            # Create slave records (PNRs) for the customer
            num_pnrs = random.randint(1, 3)
            for pnr_id in range(1, num_pnrs + 1):
                pnr = create_pnr(customer_id, f'{customer_id}-{pnr_id}', customer['passport_number'])

            # Print the generated records
            print("Master Record (Customer):", customer)
            print("Slave Records (PNRs):")
            

            # Increment customer ID
            customer_id += 1
            print('\n')
            # Wait for 1 second before generating the next record
            try:
        # Send the message
                producer.send('customers_data', value=customer)  # No need to encode manually
                producer.flush()  # Ensure all messages are sent
                print(f"Message sent to topic '{'customers_data'}': {customer}")
                producer.send('pnr_data', value=pnr)  # No need to encode manually
                producer.flush()  # Ensure all messages are sent
                print(f"Message sent to topic '{'pnr_data'}': {pnr}")
            except Exception as e:
                print(f"Failed to produce message: {e}")
           # time.sleep(30)

    except KeyboardInterrupt:
        # When the user stops the infinite loop by pressing Ctrl+C
        print("\nData generation stopped by user.")
        return customer, pnr


if __name__ == "__main__":
    generate_data(producer)
