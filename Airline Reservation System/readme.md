# Airline Data Simulation with Kafka

This project simulates the generation of airline-related data (passengers, aircrafts, flights, bookings, tickets, crew members, and flight crew assignments) using the `Faker` library, and streams this data to different Kafka topics using Python.

## Kafka Topics Used

The data is streamed to different Kafka topics based on the type of entity being generated. Below are the topics and their corresponding data:

### 1. **Passengers Topic**
   - **Topic Name:** `passengers`
   - **Data Sent:** Information about passengers, including their personal details and passport information.
   - **Fields:** `passenger_id`, `first_name`, `last_name`, `email`, `phone_number`, `passport_number`, `nationality`, `date_of_birth`

### 2. **Aircrafts Topic**
   - **Topic Name:** `aircrafts`
   - **Data Sent:** Information about aircrafts, including their model, capacity, and manufacturer.
   - **Fields:** `aircraft_id`, `model`, `capacity`, `manufacturer`

### 3. **Flights Topic**
   - **Topic Name:** `flights`
   - **Data Sent:** Details of flights, including departure and arrival airports, timings, and status.
   - **Fields:** `flight_id`, `aircraft_id`, `flight_number`, `departure_airport`, `arrival_airport`, `departure_time`, `arrival_time`, `duration`, `status`

### 4. **Bookings Topic**
   - **Topic Name:** `bookings`
   - **Data Sent:** Booking details for passengers, including flight information and total amount.
   - **Fields:** `booking_id`, `passenger_id`, `flight_id`, `booking_date`, `booking_status`, `total_amount`

### 5. **Tickets Topic**
   - **Topic Name:** `tickets`
   - **Data Sent:** Information about tickets, including seat number, ticket class, and fare.
   - **Fields:** `ticket_id`, `booking_id`, `seat_number`, `ticket_class`, `fare`, `ticket_status`

### 6. **Crew Topic**
   - **Topic Name:** `crew`
   - **Data Sent:** Information about the flight crew, including their positions and license numbers.
   - **Fields:** `crew_id`, `first_name`, `last_name`, `position`, `license_number`

### 7. **Flight Crew Assignments Topic**
   - **Topic Name:** `flight_crew`
   - **Data Sent:** Information about crew assignments for specific flights.
   - **Fields:** `flight_crew_id`, `flight_id`, `crew_id`, `duty`

This schema design manages passengers, flights, bookings, tickets, crew details, and other relevant information for an airline reservation system.

https://github.com/AzureDataAnalytics/Kafka/issues/13
