import random
from faker import Faker
import json
from datetime import datetime
from kafka import KafkaProducer


# Initialize Faker
fake = Faker()

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

truck_makes_models = [
    # Ford
    "Ford F-150",
    "Ford F-250 Super Duty",
    "Ford F-350 Super Duty",
    "Ford Ranger",
    "Ford Maverick",
    "Ford Transit",
    "Ford E-350",
    
    # Chevrolet
    "Chevrolet Silverado 1500",
    "Chevrolet Silverado 2500HD",
    "Chevrolet Silverado 3500HD",
    "Chevrolet Colorado",
    "Chevrolet Kodiak",
    "Chevrolet Express Cargo",
    
    # Dodge/Ram
    "Ram 1500",
    "Ram 2500",
    "Ram 3500",
    "Ram ProMaster",
    "Ram ProMaster City",
    
    # GMC
    "GMC Sierra 1500",
    "GMC Sierra 2500HD",
    "GMC Sierra 3500HD",
    "GMC Canyon",
    "GMC Savana Cargo",
    
    # Toyota
    "Toyota Tacoma",
    "Toyota Tundra",
    "Toyota Hilux",
    "Toyota Land Cruiser Pickup",
    
    # Nissan
    "Nissan Frontier",
    "Nissan Titan",
    "Nissan Titan XD",
    "Nissan NV Cargo",
    
    # Isuzu
    "Isuzu NPR",
    "Isuzu NQR",
    "Isuzu NRR",
    "Isuzu FTR",
    "Isuzu Elf",
    "Isuzu Giga",
    
    # Mack
    "Mack Anthem",
    "Mack Pinnacle",
    "Mack Granite",
    "Mack LR",
    "Mack TerraPro",
    "Mack MD Series",
    
    # Volvo
    "Volvo VNL Series",
    "Volvo VNR Series",
    "Volvo VNX Series",
    "Volvo FMX",
    "Volvo FH Series",
    
    # Freightliner
    "Freightliner Cascadia",
    "Freightliner M2 106",
    "Freightliner 114SD",
    "Freightliner 122SD",
    "Freightliner Coronado",
    "Freightliner EconicSD",
    
    # Kenworth
    "Kenworth T680",
    "Kenworth T880",
    "Kenworth W900",
    "Kenworth T370",
    "Kenworth T270",
    "Kenworth K270",
    
    # Peterbilt
    "Peterbilt 579",
    "Peterbilt 389",
    "Peterbilt 567",
    "Peterbilt 520",
    "Peterbilt 220",
    "Peterbilt 337",
    
    # International
    "International LT Series",
    "International RH Series",
    "International HV Series",
    "International MV Series",
    "International LoneStar",
    "International ProStar",
    
    # Western Star
    "Western Star 4900",
    "Western Star 5700XE",
    "Western Star 6900",
    "Western Star 47X",
    "Western Star 49X",
    
    # Scania
    "Scania P-Series",
    "Scania G-Series",
    "Scania R-Series",
    "Scania S-Series",
    "Scania XT",
    
    # MAN
    "MAN TGX",
    "MAN TGS",
    "MAN TGM",
    "MAN TGL",
    
    # Mercedes-Benz
    "Mercedes-Benz Actros",
    "Mercedes-Benz Arocs",
    "Mercedes-Benz Atego",
    "Mercedes-Benz Sprinter",
    "Mercedes-Benz X-Class",
    
    # Hyundai
    "Hyundai Porter II",
    "Hyundai Xcient",
    "Hyundai Mighty",
    
    # Tata
    "Tata LPT 1615",
    "Tata Signa 4923.S",
    "Tata Prima 4025.S",
    
    # Mahindra
    "Mahindra Bolero Pik-Up",
    "Mahindra Furio 7",
    "Mahindra Blazo X 42",
    
    # Hino
    "Hino 268A",
    "Hino 195",
    "Hino 155",
    "Hino 338",
    "Hino 500 Series",
    
    # UD Trucks (Nissan Diesel)
    "UD Trucks Quon",
    "UD Trucks Condor"
]


# Define the number of records to generate
NUM_SHIPMENTS = 1000
NUM_TRUCKS = 200
NUM_ROUTES = 200
NUM_TRACKING_EVENTS = 150
NUM_DRIVERS = 200
NUM_ASSIGNMENTS = 200
NUM_ALERTS = 400

def generate_shipments():
    shipments = []
    for _ in range(NUM_SHIPMENTS):
        shipment = {
            "shipment_id": fake.random_int(min=1000, max=9999),
            "origin": fake.city(),
            "destination": fake.city(),
            "current_status": random.choice(['in transit', 'delivered', 'pending']),
            "estimated_delivery": fake.date_time_between(start_date='now', end_date='+5d').isoformat(),
            "weight": round(random.uniform(1.0, 100.0), 2)
        }
        shipments.append(shipment)
        producer.send('shipments', shipment)
    return shipments

def generate_trucks():
    trucks = []
    for _ in range(NUM_TRUCKS):
        truck = {
            "truck_id": fake.random_int(min=1000, max=9999),
            "license_plate": fake.license_plate(),
            "model": random.choice(truck_makes_models),
            "capacity": round(random.uniform(5.0, 20.0), 2),
            "current_location": fake.address()
        }
        trucks.append(truck)
        producer.send('trucks', truck)
    return trucks

def generate_routes():
    routes = []
    for _ in range(NUM_ROUTES):
        route = {
            "route_id": fake.random_int(min=1000, max=9999),
            "origin": fake.city(),
            "destination": fake.city(),
            "planned_path": fake.text(),
            "estimated_time": f"{random.randint(1, 12)}h {random.randint(0, 59)}m"
        }
        routes.append(route)
        producer.send('routes', route)
    return routes

def generate_tracking_events(trucks, shipments):
    tracking_events = []
    for _ in range(NUM_TRACKING_EVENTS):
        event = {
            "event_id": fake.random_int(min=1000, max=9999),
            "timestamp": datetime.now().isoformat(),
            "truck_id": random.choice(trucks)['truck_id'],
            "shipment_id": random.choice(shipments)['shipment_id'],
            "location": fake.address(),
            "event_type": random.choice(['loaded', 'unloaded', 'delayed', 'in transit']),
            "speed": round(random.uniform(30.0, 120.0), 2)
        }
        tracking_events.append(event)
        producer.send('tracking_events', event)
    return tracking_events

def generate_drivers():
    drivers = []
    for _ in range(NUM_DRIVERS):
        driver = {
            "driver_id": fake.random_int(min=1000, max=9999),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "license_number": fake.license_plate(),
            "contact_number": fake.phone_number()
        }
        drivers.append(driver)
        producer.send('drivers', driver)
    return drivers

def generate_truck_driver_assignments(trucks, drivers):
    assignments = []
    for _ in range(NUM_ASSIGNMENTS):
        assignment = {
            "assignment_id": fake.random_int(min=1000, max=9999),
            "truck_id": random.choice(trucks)['truck_id'],
            "driver_id": random.choice(drivers)['driver_id'],
            "assignment_date": datetime.now().isoformat()
        }
        assignments.append(assignment)
        producer.send('truck_driver', assignment)
    return assignments

def generate_alerts(trucks, shipments):
    alerts = []
    for _ in range(NUM_ALERTS):
        alert = {
            "alert_id": fake.random_int(min=1000, max=9999),
            "timestamp": datetime.now().isoformat(),
            "truck_id": random.choice(trucks)['truck_id'],
            "shipment_id": random.choice(shipments)['shipment_id'],
            "alert_type": random.choice(['route deviation', 'speeding']),
            "description": fake.text(max_nb_chars=100)
        }
        alerts.append(alert)
        producer.send('alerts', alert)
    return alerts

if __name__ == "__main__":
    # Generate and send data to Kafka topics
    trucks = generate_trucks()
    shipments = generate_shipments()
    routes = generate_routes()
    tracking_events = generate_tracking_events(trucks, shipments)
    drivers = generate_drivers()
    truck_driver_assignments = generate_truck_driver_assignments(trucks, drivers)
    alerts = generate_alerts(trucks, shipments)
    print(f'Trucks:{trucks}\n')
    print(f'shipments:{shipments}\n')
    print(f'routes:{routes}\n')
    print(f'tracking_events:{tracking_events}\n')
    print(f'drivers:{drivers}\n')
    print(f'truck_driver_Assignments:{truck_driver_assignments}\n')
    print(f'alerts:{alerts}\n')

    print("Data generation complete.")
