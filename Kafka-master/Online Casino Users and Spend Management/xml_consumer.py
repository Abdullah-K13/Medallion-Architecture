import os
import json
from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
conf = {
    'bootstrap.servers': '158.220.124.0:9094',
    'group.id': 'casino_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Subscribe to the topic
topic = 'casino_topic'
consumer.subscribe([topic])

xml_file_path = 'casino_data.xml'
schema_file_path = 'casino_data_schema.xsd'

def save_files(xml_content, schema_content):
    if xml_content is not None:
        with open(xml_file_path, 'w') as xml_file:
            xml_file.write(xml_content)
            print(f'Updated XML file: {xml_file_path}')
    else:
        print('No XML content provided to save.')

    if schema_content is not None:
        with open(schema_file_path, 'w') as schema_file:
            schema_file.write(schema_content)
            print(f'Updated XSD file: {schema_file_path}')
    else:
        print('No XSD content provided to save.')

# Consume messages
try:
    print("Waiting for messages...")
    while True:
        msg = consumer.poll(1.0)  # Wait for a message
        print("Polling...")  # Debug statement to indicate polling

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue  # End of partition event
            else:
                print(f'Error: {msg.error()}')
                break

        key = msg.key().decode('utf-8') if msg.key() is not None else None
        print(f"Received message with key: {key}")
        
        # Process the message
        combined_message = json.loads(msg.value().decode('utf-8'))
        xml_content = combined_message.get('xml_data')  # Extract XML content
        schema_content = combined_message.get('xml_schema')  # Extract XSD content

        print(xml_content)

        # Save the content to files
        save_files(xml_content, schema_content)

except KeyboardInterrupt:
    print("Consumer interrupted by user.")
finally:
    consumer.close()
