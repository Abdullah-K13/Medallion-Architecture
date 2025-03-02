# Casino Data Generation & Kafka Topics

This project simulates a casino environment by generating mock data for users, games, transactions, and user game history, and sends the data to various Kafka topics using the Faker library and Python.

## Kafka Topics

1. **casino_users**
   - Contains user information such as:
     - `user_id`: Unique identifier for each user.
     - `username`: The username of the user.
     - `email`: User's email address.
     - `password_hash`: Hash of the user's password.
     - `balance`: User's account balance.
     - `registration_date`: Date and time of user registration.
     - `status`: Status of the user (e.g., 'active' or 'suspended').

2. **casino_games**
   - Contains information about casino games, including:
     - `game_id`: Unique identifier for each game.
     - `name`: Name of the game.
     - `type`: Type of game (e.g., 'slot', 'blackjack', 'roulette', etc.).
     - `min_bet`: Minimum bet allowed for the game.
     - `max_bet`: Maximum bet allowed for the game.
     - `provider`: Name of the game provider.

3. **casino_transactions**
   - Contains transactional data, such as:
     - `transaction_id`: Unique identifier for each transaction.
     - `user_id`: Identifier for the user who initiated the transaction.
     - `amount`: Transaction amount.
     - `transaction_type`: Type of transaction (e.g., 'deposit', 'withdrawal', 'bet', 'win').
     - `transaction_date`: Date and time of the transaction.
     - `game_id`: Game ID associated with the transaction (if applicable).
     - `status`: Status of the transaction (e.g., 'completed' or 'pending').

4. **casino_user_game_history**
   - Contains the history of user bets and game outcomes, including:
     - `history_id`: Unique identifier for each game history record.
     - `user_id`: Identifier for the user who played the game.
     - `game_id`: Identifier for the game played.
     - `bet_amount`: Amount the user bet on the game.
     - `win_amount`: Amount the user won (if any).
     - `game_result`: Result of the game ('win' or 'loss').
     - `game_date`: Date and time when the game was played.

---

## Usage

This project generates realistic casino data and streams it into Kafka topics for further processing and analysis. It includes user information, game details, financial transactions, and game history, all of which can be used for analytics, monitoring, or other use cases in a casino management system.

## Data Generation

The following data is generated:
- **Users**: Simulated player profiles.
- **Games**: Various casino games like slots, blackjack, and poker.
- **Transactions**: Records of deposits, withdrawals, bets, and wins.
- **User Game History**: Detailed records of user bets, wins, and losses over time.

The data is automatically streamed to the specified Kafka topics after generation.


This schema design manages online casino users and their spending. It includes tables for users, transactions, games, and related data.

https://github.com/AzureDataAnalytics/Kafka/issues/11
