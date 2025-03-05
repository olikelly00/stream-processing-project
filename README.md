# Data Engineering Workflows for E-Commerce

## Contributors

- [Edith Cheler](https://github.com/edithche)
- [Oli Kelly](https://github.com/olikelly00)

## Overview

This project implements data engineering workflows to support a fictional e-commerce platform. It processes event data using Apache Kafka, transforms it with PySpark, and stores results in an AWS-hosted analytical database. The system enables real-time fraud detection, marketing attribution, and structured event storage for business insights.

## System Architecture

Here is a diagram illustrating how the components of this project interact:

*(Add architecture diagram here)*

## Stack

### Languages, Libraries, and Frameworks

- **Python 3** – General scripting and data processing
- **SQL** – Querying and storing data in PostgreSQL
- **PySpark** – Extracting, transforming, and loading (ETL) data efficiently
- **Apache Kafka** – Event streaming for real-time data processing
- **Kafka UI** – An open-source dashboarding solution for visualising Kafka event streams
- **Confluent Kafka** – Python library for interacting with Kafka
- **Psycopg2** – PostgreSQL database adapter for Python

### Cloud Services

- **AWS EC2** – Hosting data pipelines
- **AWS MSK (Managed Streaming for Apache Kafka)** – Hosting a Kafka cluster
- **AWS RDS (Relational Database Service)** – Storing processed event data

## File Structure

### `consumer.py`

Retrieves data from a Kafka cluster hosted on AWS MSK, converts it from bytes to a PySpark DataFrame for transformation, redacts email addresses for privacy, and streams the cleaned data to a new Kafka topic (`processed_events`).

#### Technical Challenge

Establishing a secure connection to AWS MSK was a new challenge. MSK uses IAM authentication, requiring precise connection parameters. Through trial and error, AWS documentation, and debugging error messages, we successfully connected to the cluster.

---

### `database_consumer.py`

Retrieves event data from Kafka, converts it to a PySpark DataFrame, and loads it into an analytical database hosted on AWS RDS.

#### Technical Challenge

Setting up a connection to the AWS RDS database with PySpark was unfamiliar territory. We had to research the correct JDBC (Java Database Connection) syntax used under the hood for Kafka-Postgres communication. By iterating on our syntax and troubleshooting error messages, we successfully stored event data in the database.

---

### `fraud_detection.py`

Detects fraudulent activity by tracking rapid add-to-cart events. If a user adds five items to their cart in less than five seconds, a fraud alert is generated and streamed to Kafka.

#### Technical Challenge

Ensuring the tracker reset correctly every five seconds while still accepting new events was tricky. We considered different data structures (e.g., dictionaries and double-ended queues) and ultimately used a dictionary for simplicity. To allow concurrent updates and resets, we leveraged Python’s `threading` library. This solution was more resource-intensive but ensured reliable fraud detection. We verified the logic using mock event data.

---

### `marketing_attribution.py`

Attributes customer orders to the marketing channel through which they first entered the site. When an order is placed, this script retrieves the user's first `site_visit` event and stores the user ID and marketing channel in an AWS RDS database for future analysis.

#### Technical Challenge

Finding a user's first event required tracking `site_visit` and `order_confirmed` events efficiently. We implemented a dictionary (`user_tracker`) to store the first marketing channel associated with a user. If an event with `event_name == 'visit'` was encountered and the user was not already in `user_tracker`, we stored the marketing channel. When an `order_confirmed` event occurred, we retrieved the corresponding channel from `user_tracker`, defaulting to 'organic' if no prior visit was recorded. This was implemented using PySpark's `foreachBatch` to process event data efficiently and store results in an AWS RDS database.

## Key Learnings

- **Debugging AWS IAM authentication issues** – Trial and error with IAM roles and permissions was key to connecting with MSK securely.
- **Understanding PySpark for data transformations** – Implementing ETL pipelines in PySpark expanded our knowledge of batch and streaming data processing.
- **Designing data pipelines to efficiently process and respond to event-driven streams** – Kafka’s event streaming model required careful structuring of data ingestion and transformation processes.
- **Handling concurrent processes in Python** – Using `threading` allowed us to manage real-time event tracking efficiently.

## Future Improvements

- **Optimise fraud detection logic** – Experiment with sliding windows instead of fixed intervals for more flexible fraud detection.
- **Implementing a monitoring tool** – Experiment with open-source monitoring libraries for more visibility of how well our stream processing infrastructure is working.
- **Enhance security** – Implement encryption for sensitive user data before sending it to Kafka.