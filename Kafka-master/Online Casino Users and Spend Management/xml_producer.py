import json
from confluent_kafka import Producer

# Kafka producer configuration
conf = {
    'bootstrap.servers': '158.220.124.0:9094',  # Update with your Kafka broker address
}

producer = Producer(**conf)

def send_casino_data(xml_file_path, schema_file_path):
    # Read the XML file
    with open(xml_file_path, 'r') as xml_file:
        xml_content = xml_file.read()

    # Read the XML schema file
    with open(schema_file_path, 'r') as schema_file:
        schema_content = schema_file.read()

    # Create a combined message
    combined_message = {
        'xml_data': xml_content,
        'xml_schema': schema_content
    }

    # Produce the combined message
    producer.produce('casino_topic',  value=json.dumps(combined_message))
    producer.flush()
    print("Sent updated casino data and schema.")

# Specify the paths to your XML and schema files
xml_file_path = 'casino_data.xml'
schema_file_path = 'casino_data_schema.xsd'

# Send the data
send_casino_data(xml_file_path, schema_file_path)
