# Kafka Topics for Data Generation

This project generates and sends mock trucking and shipment data to various Kafka topics using Python and Faker. The data is structured into different categories and is sent to the following Kafka topics:

## Kafka Topics

1. **shipments**
   - Contains information about individual shipments, including shipment ID, origin, destination, status, estimated delivery time, and weight.

2. **trucks**
   - Contains data about trucks such as truck ID, license plate, model, capacity, and current location.

3. **routes**
   - Contains information about planned routes, including route ID, origin, destination, planned path, and estimated time.

4. **tracking_events**
   - Contains tracking events for trucks and shipments, including event ID, timestamp, truck ID, shipment ID, location, event type, and speed.

5. **drivers**
   - Contains information about truck drivers, including driver ID, name, license number, and contact information.

6. **truck_driver**
   - Contains truck-to-driver assignment data, including assignment ID, truck ID, driver ID, and assignment date.

7. **alerts**
   - Contains alert data for shipments and trucks, including alert ID, timestamp, truck ID, shipment ID, alert type, and description.

---

## Usage

This code simulates data for a logistics and trucking system and streams it into Kafka topics for further processing, analysis, or monitoring. Each topic corresponds to a particular data category, ensuring that the information is well-organized and scalable.


Schema designed for a logistics company that tracks packets and trucks in real-time using streaming data. It includes tables for shipments, trucks, tracking events, and routes.

https://github.com/AzureDataAnalytics/Kafka/issues/14
