## Files Overview

### 1. `AggregatingTable.py`

This file is responsible for aggregating data from Delta tables using PySpark in a Databricks environment. The script:

- Reads data from Delta tables.
- Applies necessary transformations and aggregation logic.
- Writes the aggregated data back into Delta tables for further analysis.

#### Key Components:
- **Delta Tables:** Input and output data sources.
- **PySpark:** Used for executing queries and performing transformations.
- **Databricks:** The environment where the file is run.

### 2. `ToParseKafkaMessageToTables.py`

This file consumes data in **batch mode** from an Apache Kafka topic and dumps it into a Delta table. The file:

- Connects to a Kafka topic and consumes messages in batches.
- Parses and processes the data.
- Writes the processed data into a Delta table for storage and future analysis.

#### Key Components:
- **Kafka:** Source of the streaming data.
- **Batch Mode:** Consumes messages in defined batches.
- **Delta Tables:** Destination for the processed data.

### 3. `ConsumerForKafka.py`

This file is designed for **real-time streaming** data from a Kafka topic and displaying it on the console. The file:

- Connects to a Kafka topic to continuously consume messages in real-time.
- Prints the data to the console for monitoring purposes.

#### Key Components:
- **Kafka:** Source of real-time data.
- **Streaming Mode:** Data is consumed continuously as new messages arrive.
- **Console Output:** Data is displayed on the console.

### 4. `BronzeLayer.py`

This script combines Kafka and Delta Lake functionalities by:

- Consuming data from Kafka.
- Processing the data (transformations, filtering, etc.).
- Dumping the data into a Delta table for storage and future usage.

#### Key Components:
- **Kafka:** Source of the data.
- **Delta Tables:** Destination for storing processed messages.

  ### 5. `WriteKafkaMessagesToFiles.py`

This script combines Kafka and Delta Lake functionalities by:

- Consuming data from Kafka.
- Processing the data (transformations, filtering, etc.).
- Writing data to parquet and csv files.

#### Key Components:
- **Kafka:** Source of the data.
- **Delta Tables:** Destination for storing processed messages.

## Getting Started

### Prerequisites
- **Apache Kafka**: Make sure Kafka is running and accessible.
- **Databricks**: You need access to a Databricks workspace.
- **PySpark**: Ensure PySpark is installed if running locally.
- **Delta Lake**: Delta tables should be configured in your environment.

### Usage

1. **Aggregating Data with Databricks:**
   Run `databricks_aggregation.py` in the Databricks notebook or workspace to aggregate data from Delta tables.

2. **Batch Consumption from Kafka:**
   Use `kafka_batch_consumer.py` to consume data from a Kafka topic in batch mode and dump it into a Delta table:
   ```bash
   python kafka_batch_consumer.py
