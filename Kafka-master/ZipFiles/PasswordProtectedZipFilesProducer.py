import json
import os
import zipfile
import random
import string
from confluent_kafka import Producer
from faker import Faker
import io
# Initialize Faker for generating random customer data
faker = Faker()

# Confluent Kafka Producer Configuration
conf = {
    'bootstrap.servers': '158.220.124.0:9094',  # Replace with your Kafka broker address
    'client.id': 'python-producer',
    'acks': 'all'  # Wait for all replicas to acknowledge the message
}

# Initialize Confluent Kafka Producer
producer = Producer(conf)

# Folder to store temporary files
temp_folder = '/tmp/customer_data/'

# Ensure the temporary folder exists
os.makedirs(temp_folder, exist_ok=True)

# Function to generate random password
def generate_password(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Function to create a password-protected zip file
def create_password_protected_zip(file_path, password, json_data):
    # Save the JSON data to a file
    json_file_path = os.path.join(temp_folder, 'customer_data.json')
    with open(json_file_path, 'w') as f:
        json.dump(json_data, f)

    # Create the password-protected zip file
    with zipfile.ZipFile(file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.setpassword(password.encode('utf-8'))
        zipf.write(json_file_path, arcname='customer_data.json')

    # Remove the unzipped JSON file
    os.remove(json_file_path)

# Function to generate fake customer data
def generate_customer_data():
    return {
        "name": faker.name(),
        "address": faker.address(),
        "phone_number": faker.phone_number(),
        "email": faker.email(),
        "company": faker.company(),
    }

# Delivery report callback function
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to continuously send data
def produce_data():
    while True:
        # Generate fake customer data
        customer_data = generate_customer_data()

        # Create a new password for each zip file
        password = generate_password()

        # Create zip file in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.setpassword(password.encode('utf-8'))
            zipf.writestr('customer_data.json', json.dumps(customer_data))

        # Get the zip content as bytes
        zip_content = zip_buffer.getvalue()

        # Prepare message to send, including the hex-encoded zip file and password
        message = {
            'zip_file': zip_content.hex(),  # Convert binary content to hex string
            'password': password
        }

        # Produce the message to the 'customer_data' topic in Kafka
        producer.produce('customer_data', value=json.dumps(message), callback=delivery_report)
        producer.flush()

        print(f"Sent zip file with password: {password}")

# Start producing data
produce_data()
