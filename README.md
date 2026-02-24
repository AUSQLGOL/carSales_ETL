# Car Sales ETL Pipeline
By Austin Golcher

## Project Proposal
This project implements a production-style end-to-end ETL data pipeline that processes over 550,000 car sales records, performs data validation and transformation, loads structured data into PostgreSQL, executes advanced analytical queries, and exports results to AWS S3.

The goal of this project was to simulate a real-world data engineering workflow including:

*Large-scale batch ingestion
*Data cleaning & validation
*Schema design
*Bulk database insertion
*Analytical SQL
*Cloud storage integration
*Logging & retry fault tolerance

## Architecture
 Raw CSV (550k+ records)
           ↓
   Python ETL Script
           ↓
Data Cleaning & Validation
           ↓
       PostgreSQL
           ↓
 Analytical SQL Queries
           ↓
 Export Processed Data
           ↓
     AWS S3 Upload

## Technologies Used
* Python
* PostgreSQL
* psycopg2 (bulk inserts with execute_values)
* boto3 (AWS S3 integration)
* dotenv (environment management)
* Logging (error tracking)
* Retrying (fault tolerance)
* Advanced SQL (CTEs, Window Functions, Aggregations)

## Dataset
The dataset taken from Kaggle (https://www.kaggle.com/datasets/syedanwarafridi/vehicle-sales-data) contains over 550,000 car sales transactions with fields such as:
* Vehicle details (year, make, model, trim)
* VIN (unique identifier)
* Odometer mileage
* MMR (estimated market value)
* Selling price
* Sale date
* State and seller information

## ETL Process
1) Extract
* Reads raw CSV file
* Handles large-volume batch processing

2) Transform
* Cleans missing and empty values
* Parses complex timestamp formats
* Enforces data types (INT, TIMESTAMP)
* Normalizes column names
* Handles duplicates using ON CONFLICT DO NOTHING
* Applies validation rules for numeric fields
* Logs row-level processing errors

3) Load
* Creates relational schema if not exists
* Inserts data using optimized bulk operations
* Commits transactions safely
* Implements connection retries

## Environment Configuration
In case you want to run this project you will need to modify your routes from your S3 Cloud and localfiles:
* S3_BUCKET_NAME = 'YOUR_BUCKET_NAME'
* LOCAL_CSV_FILE = r'C:\Users\YOUR_USER_NAME\Desktop\carSales_ETL\car_prices.csv'
* PROCESSED_CSV_FILE = r'C:\Users\YOUR_USER_NAME\Desktop\carSales_ETL\processed_data.csv'

As well as in the .env file:

* DB_USER=your_user
* DB_PASS=your_password
* DB_HOST=localhost