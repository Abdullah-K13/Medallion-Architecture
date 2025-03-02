import json
from faker import Faker
from kafka import KafkaProducer
import random
from datetime import datetime, timedelta
import time

# Initialize Faker and Kafka producer
fake = Faker()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Constants
num_residents = 1000
num_contractors = 500
num_third_parties = 250
num_issues = 500
num_apartments = 600

# Generate apartments
apartments = []
for i in range(1, num_apartments + 1):
    apartments.append({
        'apartment_id': i,
        'block': fake.word() + random.choice(['$', '%', '123']),  # Adding special chars
        'apartment_number': str(i) + random.choice(['a', 'b']),  # Invalid number type
        'floor': random.choice([random.randint(1, 10), 'ten'])  # Randomly injecting strings
    })

# Generate residents with unclean data
residents = []
for i in range(1, num_residents + 1):
    residents.append({
        'resident_id': i,
        'first_name': fake.first_name() + random.choice(['@', '12']),  # Adding invalid characters
        'last_name': fake.last_name() + random.choice(['#', '99']),  # Adding numbers
        'email': fake.email().replace('@', random.choice(['$', '&'])),  # Invalid email address
        'phone_number': fake.phone_number() + random.choice(['abc', 'xyz']),  # Invalid phone number
        'apartment_id': random.choice(apartments)['apartment_id']
    })

# Generate contractors with unclean data
contractors = []
for i in range(1, num_contractors + 1):
    contractors.append({
        'contractor_id': i,
        'name': fake.company() + random.choice([' Ltd', ' Inc', '##']),  # Adding special chars
        'service_type': random.choice(['plumbing', 'electrical', 'cleaning', 'landscaping']) + str(random.randint(0, 9)),  # Random numbers added
        'contact_number': fake.phone_number().replace("-", random.choice(["!", "@"])),  # Adding special characters
        'email': fake.email().replace('.', random.choice([';', ' ']))  # Invalid email format
    })

# Generate third parties with unclean data
third_parties = []
for i in range(1, num_third_parties + 1):
    third_parties.append({
        'third_party_id': i,
        'name': fake.company() + random.choice(['%', '123']),  # Adding special chars
        'service_type': random.choice(['insurance', 'city utilities', 'repair service']) + 'X',  # Invalid service type
        'contact_number': fake.phone_number() + '###',  # Adding invalid data
        'email': fake.email().replace('@', 'at')  # Invalid email format
    })

# Generate issues with unclean data
issues = []
for i in range(1, num_issues + 1):
    reported_date = fake.date_time_this_year()
    issues.append({
        'issue_id': i,
        'resident_id': random.choice(residents)['resident_id'],
        'reported_date': reported_date.strftime('%Y-%m-%d %H:%M:%S'),
        'issue_type': random.choice(['plumbing', 'electrical', 'internet', 'heating']) + random.choice(['1', '?']),  # Invalid issue type
        'description': fake.sentence() + random.choice(['!!!', '@@@']),  # Adding special characters
        'status': random.choice(['reported', 'in progress', 'resolved', 'unknown status']),  # Adding invalid status
        'priority': random.choice(['low', 'medium', 'high', 'urgent'])  # Random extra priority
    })

# Generate issue assignments
issue_assignments = []
for issue in issues:
    reported_date = datetime.strptime(issue['reported_date'], '%Y-%m-%d %H:%M:%S')
    expected_completion = (reported_date + timedelta(days=random.randint(1, 5))).strftime('%Y-%m-%d %H:%M:%S')
    
    issue_assignments.append({
        'assignment_id': issue['issue_id'],
        'issue_id': issue['issue_id'],
        'contractor_id': random.choice(contractors)['contractor_id'],
        'third_party_id': random.choice(third_parties)['third_party_id'],
        'assigned_date': issue['reported_date'],
        'expected_completion': expected_completion,
        'actual_completion': None,
        'role': random.choice(['main contractor', 'subcontractor', 'helper'])  # Adding extra roles
    })

# Generate charges
charges = []
for issue in issues:
    reported_date = datetime.strptime(issue['reported_date'], '%Y-%m-%d %H:%M:%S')
    charge_date = (reported_date + timedelta(days=random.randint(0, 10))).strftime('%Y-%m-%d %H:%M:%S')
    
    charges.append({
        'charge_id': issue['issue_id'],
        'issue_id': issue['issue_id'],
        'contractor_id': random.choice(contractors)['contractor_id'],
        'third_party_id': random.choice(third_parties)['third_party_id'],
        'charge_amount': round(random.uniform(50.0, 500.0), 2) + random.choice([1000, -500]),  # Adding unusually high/low values
        'charge_date': charge_date,
        'payment_status': random.choice(['pending', 'paid', 'overdue'])  # Adding extra statuses
    })


i = 0  # Initialize counter

while i < len(residents):
    # Send resident data
    producer.send('residents_topic', residents[i])

    # Send apartment data if the index is valid
    if i < num_apartments:
        producer.send('apartments_topic', apartments[i])

    # Send contractor data if the index is valid
    if i < num_contractors:
        producer.send('contractors_topic', contractors[i])

    # Send third-party data if the index is valid
    if i < num_third_parties:
        producer.send('third_parties_topic', third_parties[i])

    # Send issue, issue assignments, and charges data if the index is valid
    if i < num_issues:
        producer.send('issues_topic', issues[i])
        producer.send('issue_assignments_topic', issue_assignments[i])
        producer.send('charges_topic', charges[i])

    # Flush the producer buffer to ensure data is sent
    producer.flush()

    # Print status
    print(f"Data set {i + 1} generated and sent to Kafka successfully.")

    # Increment counter
    i += 1

    # Delay to simulate continuous streaming
    time.sleep(1)

# After sending all data, close the producer
# Flush and close the producer
producer.flush()
producer.close()

print("Unclean data generated and sent to Kafka successfully.")
