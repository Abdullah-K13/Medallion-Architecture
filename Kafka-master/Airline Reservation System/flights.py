import random
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
import json

# Initialize Faker
fake = Faker()

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='158.220.124.0:9092',  # Replace with your Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generate sample data functions

city_abbreviations = [
    "ATL",  # Atlanta, Georgia, USA
    "LAX",  # Los Angeles, California, USA
    "ORD",  # Chicago, Illinois, USA
    "DFW",  # Dallas/Fort Worth, Texas, USA
    "DEN",  # Denver, Colorado, USA
    "JFK",  # New York City, New York, USA
    "SEA",  # Seattle, Washington, USA
    "SFO",  # San Francisco, California, USA
    "LAS",  # Las Vegas, Nevada, USA
    "MIA",  # Miami, Florida, USA
    "PHX",  # Phoenix, Arizona, USA
    "BOS",  # Boston, Massachusetts, USA
    "IAH",  # Houston, Texas, USA
    "MSP",  # Minneapolis, Minnesota, USA
    "CLT",  # Charlotte, North Carolina, USA
    "DTW",  # Detroit, Michigan, USA
    "PHL",  # Philadelphia, Pennsylvania, USA
    "BWI",  # Baltimore/Washington International, Maryland, USA
    "MCO",  # Orlando, Florida, USA
    "SAN",  # San Diego, California, USA
    "TPA",  # Tampa, Florida, USA
    "SLC",  # Salt Lake City, Utah, USA
    "HNL",  # Honolulu, Hawaii, USA
    "YYZ",  # Toronto, Ontario, Canada
    "YVR",  # Vancouver, British Columbia, Canada
    "LHR",  # London Heathrow, United Kingdom
    "CDG",  # Charles de Gaulle Airport, Paris, France
    "FRA",  # Frankfurt Airport, Germany
    "NRT",  # Narita International Airport, Tokyo, Japan
    "HKG",  # Hong Kong International Airport
    "SYD",  # Sydney Kingsford Smith Airport, Australia
    "AKL",  # Auckland Airport, New Zealand
    "DXB",  # Dubai International Airport, UAE
    "SIN",  # Singapore Changi Airport
]

def generate_passenger():
    return {
        "passenger_id": fake.unique.random_int(min=1, max=10000),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "passport_number": fake.passport_number(),
        "nationality": fake.country(),
        "date_of_birth": fake.date_of_birth(minimum_age=18).isoformat()
    }

def generate_aircraft():
    return {
        "aircraft_id": fake.unique.random_int(min=1, max=10000),
        "model": fake.word().capitalize() + " " + fake.word().capitalize(),
        "capacity": random.randint(50, 300),
        "manufacturer": fake.company()
    }

def generate_flight(aircraft_id):
    departure_airport = random.choice(city_abbreviations)
    arrival_airport = random.choice(city_abbreviations)
    departure_time = fake.date_time_this_year()
    duration_hours = random.randint(1, 10)
    
    return {
        "flight_id": fake.unique.random_int(min=1, max=10000),
        "aircraft_id": aircraft_id,
        "flight_number": fake.random_uppercase_letter() + str(fake.unique.random_int(min=100, max=9999)),
        "departure_airport": departure_airport,
        "arrival_airport": arrival_airport,
        "departure_time": departure_time.isoformat(),
        "arrival_time": (departure_time + timedelta(hours=duration_hours)).isoformat(),
        "duration": f"{duration_hours}h {random.randint(1, 59)}m",
        "status": random.choice(['on time', 'delayed', 'canceled'])
    }

def generate_booking(passenger_id, flight_id):
    return {
        "booking_id": fake.unique.random_int(min=1, max=10000),
        "passenger_id": passenger_id,
        "flight_id": flight_id,
        "booking_date": datetime.now().isoformat(),
        "booking_status": random.choice(['confirmed', 'canceled']),
        "total_amount": round(random.uniform(50.0, 1500.0), 2)
    }

def generate_ticket(booking_id):
    return {
        "ticket_id": fake.unique.random_int(min=1, max=10000),
        "booking_id": booking_id,
        "seat_number": f"{random.randint(1, 30)}{random.choice(['A', 'B', 'C', 'D', 'E', 'F'])}",
        "ticket_class": random.choice(['economy', 'business']),
        "fare": round(random.uniform(50.0, 1500.0), 2),
        "ticket_status": random.choice(['issued', 'checked-in'])
    }

def generate_crew():
    return {
        "crew_id": fake.unique.random_int(min=1, max=10000),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "position": random.choice(['pilot', 'flight attendant']),
        "license_number": fake.unique.random_int(min=1000, max=9999)
    }

def generate_flight_crew(flight_id, crew_id):
    return {
        "flight_crew_id": fake.unique.random_int(min=1, max=10000),
        "flight_id": flight_id,
        "crew_id": crew_id,
        "duty": random.choice(['captain', 'first officer', 'attendant'])
    }

# Generate data and send to Kafka
def generate_and_send_data(num_records):
    for _ in range(num_records):
        passenger = generate_passenger()
        producer.send('passengers', passenger)

        aircraft = generate_aircraft()
        producer.send('aircrafts', aircraft)

        flight = generate_flight(aircraft['aircraft_id'])
        producer.send('flights', flight)

        booking = generate_booking(passenger['passenger_id'], flight['flight_id'])
        producer.send('bookings', booking)

        ticket = generate_ticket(booking['booking_id'])
        producer.send('tickets', ticket)

        crew_member = generate_crew()
        producer.send('crew', crew_member)

        flight_crew_assignment = generate_flight_crew(flight['flight_id'], crew_member['crew_id'])
        producer.send('flight_crew', flight_crew_assignment)

# Example usage
if __name__ == "__main__":
    generate_and_send_data(1000)  # Generate and send 10 records for each table
    producer.flush()
    print("Data generation and sending complete.")
