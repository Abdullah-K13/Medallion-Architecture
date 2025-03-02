# Real-Time Data Processing with Apache Spark and Delta Lake

This repository contains a real-time data processing pipeline using Apache Spark, Kafka, and Delta Lake. The application reads data from various Kafka topics, processes it using Spark Streaming, and writes the data into corresponding Delta tables.

## Project Overview

The system ingests real-time data from various Kafka topics that represent different entities in a food delivery application. Each topic has its own schema, which is transformed and stored into Delta Lake tables. These tables can then be used for further analysis and reporting.

## Table of Contents
- [Delta Table Names](#delta-table-names)
- [Kafka Topics](#kafka-topics)

## Delta Table Names

The data from Kafka is written to the following Delta tables:

| Kafka Topic        | Delta Table Name               | Description                                 |
| ------------------ | ------------------------------ | ------------------------------------------- |
| `users_data`       | `bronze_users_table`           | Stores customer details                     |
| `restaurant_data`  | `bronze_restaurant_table`      | Stores restaurant details                   |
| `menu_items`       | `bronze_menu_items_table`      | Stores restaurant menu items                |
| `orders`           | `bronze_orders_table`          | Stores order details                        |
| `order_items`      | `bronze_order_items_table`     | Stores order items related to each order    |
| `delivery_detail`  | `bronze_delivery_details_table`| Stores delivery details for each order      |

## Kafka Topics

The following Kafka topics are used in this pipeline, each corresponding to different entities in the system:

1. **users_data**
2. **restaurant_data**
3. **menu_items**
4. **orders**
5. **order_items**
6. **delivery_detail**


Schema design for managing users and orders for an online food delivery platform. It includes tables for users, restaurants, menu items, orders, and delivery details.

https://github.com/AzureDataAnalytics/Kafka/issues/12
