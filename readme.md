# Real-Time Data Processing with Medallion Architecture Using Kafka, Microsoft Fabric & Databricks

## 🚀 Overview
This project demonstrates a **real-time data pipeline** using **Medallion Architecture** to process, transform, and analyze high-velocity data streams. It integrates **Kafka, Microsoft Fabric, and Databricks** to build scalable and efficient data pipelines for multiple industry use cases.

## 📌 Use Cases Implemented
- ✈️ **Airline PNR Processing**
- 🎰 **Online Casino User & Spend Management**
- 🏢 **Apartment Residents System**
- 🛫 **Airline Reservation System**
- 🍽️ **Online Food Delivery**

## 🔧 Architecture & Implementation
### 1️⃣ **Kafka Setup & CI/CD Automation**
- Kafka server is deployed on a **Contabo virtual machine**.
- Managed entirely through **GitHub Actions**, eliminating the need for manual VM access.
- A **Docker Compose** file allows modifications directly from GitHub.

### 2️⃣ **Real-Time Data Simulation**
- Python scripts generate and stream **hundreds of thousands of records per second**.
- Data is produced in **CSV, JSON, and XML formats**, simulating real-world transactions.

### 3️⃣ **Streaming & Transformation with Microsoft Fabric & Databricks**
- **Kafka ingests real-time data** for continuous streaming.
- **Microsoft Fabric & Databricks consume and process data**, ensuring smooth data transformation.
- Schema validation and data quality enforcement using **Great Expectations**.

### 4️⃣ **Medallion Architecture Implementation**
#### 📌 **Bronze Layer**
- Raw data ingestion into **Delta Lake**.
- Data stored without modification for future processing.

#### 📌 **Silver Layer**
- Data cleansing and transformation.
- Schema enforcement and validation to ensure consistency.

#### 📌 **Gold Layer**
- Aggregated and business-ready data tailored for each use case.
- Optimized for analytics and reporting.

## Architectire Diagram
![kafka drawio](https://github.com/user-attachments/assets/037832c9-82ac-4799-a478-06802f90778e)


## 🎯 Key Features
✅ **Fully Automated Kafka Server Management** with **GitHub Actions & Docker Compose**.  
✅ **High-Velocity Real-Time Data Processing**, handling thousands of transactions per second.  
✅ **Schema Validation & Data Quality Checks** with **Great Expectations**.  
✅ **Scalable & Efficient Data Lakehouse Implementation** using **Databricks & Microsoft Fabric**.  
✅ **End-to-End ETL Pipeline**, from ingestion to analytics-ready datasets.  

## 🛠️ Technologies Used
- **Kafka** (Message streaming platform)
- **Microsoft Fabric** (Data processing & analytics)
- **Databricks** (Data lakehouse architecture)
- **Delta Lake** (Storage & versioning)
- **Great Expectations** (Data validation)
- **GitHub Actions** (CI/CD for Kafka server management)
- **Docker & Docker Compose** (Containerization & orchestration)
- **Python** (Data simulation & processing)
- **Apache Spark** (Data transformation & aggregation)
- **AWS (S3, Glue, Redshift, QuickSight)** *(Optional integration for further analytics)*

## 🚀 How to Run the Project
### 1️⃣ **Clone the Repository**
```sh
 git clone https://github.com/Abdullah-K13/Medallion-Architecture.git
 cd medallion-architecture
```

### 2️⃣ **Set Up Kafka Server**
- Modify `docker-compose.yml` as needed.
- Push changes to GitHub to trigger the **CI/CD pipeline**.

### 3️⃣ **Run Real-Time Data Generator**
```sh
python data_generator.py
```

### 4️⃣ **Start Microsoft Fabric & Databricks Scripts**
- Configure **Databricks Notebooks** to consume data from Kafka.
- Set up **Microsoft Fabric** pipelines to process and transform data.

### 5️⃣ **Validate Data & Create Delta Tables**
- Run **Great Expectations** validation.
- Load processed data into **Delta Lake (Bronze → Silver → Gold layers)**.

### 6️⃣ **Perform Analytics & Visualization**
- Query the **Gold Layer** for business insights.
- Use **Microsoft Fabric / QuickSight for dashboards**.

## 📂 Project Structure
```
📦 medallion-architecture
├── 📂 kafka-server (Kafka setup & configuration)
├── 📂 data-generator (Python scripts for real-time data simulation)
├── 📂 databricks-notebooks (ETL processing & transformation scripts)
├── 📂 fabric-pipelines (Microsoft Fabric ingestion & processing)
├── 📜 docker-compose.yml (Kafka server automation)
├── 📜 README.md (Project documentation)
└── 📜 .github/workflows (CI/CD pipeline for Kafka server)
```

## 📢 Contributing
Want to contribute? Feel free to open an issue or submit a pull request! 🙌

## 🔗 Connect with Me
📧 Email: abdullahk4803@gmail.com

