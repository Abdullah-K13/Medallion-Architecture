import json
from kafka import KafkaProducer
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker and Kafka producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# Function to generate fake users
def generate_users(num_users):
    users = []
    for _ in range(num_users):
        user = {
            "user_id": fake.unique.random_int(min=1, max=10000),
            "username": fake.user_name(),
            "email": fake.email(),
            "password_hash": fake.sha256(),
            "balance": round(random.uniform(0, 1000), 2),
            "registration_date": fake.date_time_this_decade().isoformat(),
            "status": random.choice(['active', 'suspended']),
        }
        users.append(user)
    return users

# Function to generate fake games
def generate_games(num_games):
    games = []
    game_types = ['slot', 'blackjack', 'roulette', 'poker', 'baccarat']
    for _ in range(num_games):
        game = {
            "game_id": fake.unique.random_int(min=1, max=1000),
            "name": fake.catch_phrase(),
            "type": random.choice(game_types),
            "min_bet": round(random.uniform(1, 100), 2),
            "max_bet": round(random.uniform(100, 1000), 2),
            "provider": fake.company(),
        }
        games.append(game)
    return games

# Function to generate fake transactions
def generate_transactions(num_transactions, users, games):
    transactions = []
    for _ in range(num_transactions):
        user_id = random.choice(users)["user_id"]
        game_id = random.choice(games)["game_id"] if random.choice([True, False]) else None
        transaction = {
            "transaction_id": fake.unique.random_int(min=1, max=10000),
            "user_id": user_id,
            "amount": round(random.uniform(1, 500), 2),
            "transaction_type": random.choice(['deposit', 'withdrawal', 'bet', 'win']),
            "transaction_date": fake.date_time_this_year().isoformat(),
            "game_id": game_id,
            "status": random.choice(['completed', 'pending']),
        }
        transactions.append(transaction)
    return transactions

# Function to generate fake user game history
def generate_user_game_history(num_records, users, games):
    history = []
    for _ in range(num_records):
        user_id = random.choice(users)["user_id"]
        game_id = random.choice(games)["game_id"]
        bet_amount = round(random.uniform(1, 500), 2)
        win_amount = round(random.uniform(0, bet_amount), 2) if random.choice([True, False]) else 0.00
        game_result = 'win' if win_amount > 0 else 'loss'
        
        record = {
            "history_id": fake.unique.random_int(min=1, max=10000),
            "user_id": user_id,
            "game_id": game_id,
            "bet_amount": bet_amount,
            "win_amount": win_amount,
            "game_result": game_result,
            "game_date": fake.date_time_this_year().isoformat(),
        }
        history.append(record)
    return history

# Generate data
num_users = 1000
num_games = 200
num_transactions = 1500
num_game_history = 2000

users = generate_users(num_users)
games = generate_games(num_games)
transactions = generate_transactions(num_transactions, users, games)
user_game_history = generate_user_game_history(num_game_history, users, games)

# # Send data to Kafka
for user in users:
    producer.send('casino_users', user)
    producer.flush()

for game in games:
    producer.send('casino_games', game)
    producer.flush()

for transaction in transactions:
    producer.send('casino_transactions', transaction)
    producer.flush()

for record in user_game_history:
    producer.send('casino_user_game_history', record)
    producer.flush()

# Close the producer
producer.close()

print("Data generation and sending to Kafka completed.")
