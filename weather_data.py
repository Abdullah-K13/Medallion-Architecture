from kafka import KafkaProducer
import json
import requests
import time

# Initialize the producer
producer = KafkaProducer(
    bootstrap_servers=['158.220.124.0:9094', '158.220.124.0:9093'],  # List of brokers
    retries=5,    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON
    key_serializer=lambda k: str(k).encode('utf-8')  # Serialize keys
)

def produce_data(topic, data, producer):
    try:
        # Send the message
        producer.send(topic, value=data)  # No need to encode manually
        producer.flush()  # Ensure all messages are sent
        print(f"Message sent to topic '{topic}': {data}")
    except Exception as e:
        print(f"Failed to produce message: {e}")



def send_data(topic,producer):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        'latitude': 52.52,      
        'longitude': 13.405,   
        'hourly': 'temperature_2m,wind_speed_10m',
        'current_weather': True  
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:

        data = response.json()
        current_weather = data.get("current_weather")  
        
        if current_weather:
            current_weather_str = str(data)  
            produce_data(topic, current_weather_str, producer) 
        else:
            print("No current weather data found.")
    else:
        print(f"Error: Unable to fetch data (Status code: {response.status_code})")
   

def run_scheduler():
    producer = KafkaProducer(
    bootstrap_servers=['158.220.124.0:9094', '158.220.124.0:9093'],  # List of brokers
    retries=5,    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON
    key_serializer=lambda k: str(k).encode('utf-8')  # Serialize keys
    )
    topic = 'weather-updates'  

    start_time = time.time()
    duration = 1000  

    while time.time() - start_time < duration:
        send_data(topic,producer)
        time.sleep(60)  
    
# Run the scheduler
if __name__ == "__main__":
    run_scheduler()
