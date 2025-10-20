-- Create a storage integration object to connect snowflwke to azure blob storage
CREATE STORAGE INTEGRATION azure_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = AZURE
ENABLED = TRUE
AZURE_TENANT_ID = '4f76d3ed-73a3-4ae8-9e8f-8d39080d6434'
STORAGE_ALLOWED_LOCATIONS = ('azure://snowstorage11.blob.core.windows.net/raw');

-- Confirm integration and the retrned details
DESC INTEGRATION azure_integration;

-- Create the database
CREATE DATABASE IF NOT EXISTS logistics_db;

-- Create the schema
--CREATE SCHEMA IF NOT EXISTS logistics_db.azure_integration;

CREATE SCHEMA IF NOT EXISTS logistics_db.raw;

--Create file format
CREATE OR REPLACE FILE FORMAT logistics_db.raw.csv_file_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE;

-- Create an external stage in snowflake to access the blob storage on azure
-- Use file format and integration to create stage object

CREATE OR REPLACE STAGE logistics_db.raw.logistics_container_stage
URL = 'azure://snowstorage11.blob.core.windows.net/raw'
STORAGE_INTEGRATION = azure_integration
FILE_FORMAT = logistics_db.raw.csv_file_format;

-- List files in the staging area
LIST @logistics_db.raw.logistics_container_stage;

-- Load data into snowflake tables

-- Create customers table
CREATE OR REPLACE TABLE logistics_db.raw.customers(
    CUSTOMER_ID INT,
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    EMAIL VARCHAR(50),
    PHONE_NUMBER VARCHAR(50),
    CITY VARCHAR(50),
    SIGNUP_DATE DATE,
    LAST_UPDATED_TIMESTAMP DATE
);

-- Copy data from azure blob storage into the customers table
COPY INTO logistics_db.raw.customers
    FROM @logistics_db.raw.logistics_container_stage
    FILES = ('snow_customers.csv')
    FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Create drivers table
CREATE OR REPLACE TABLE logistics_db.raw.drivers(
    DRIVER_ID INT,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    PHONE_NUMBER VARCHAR,
    VEHICLE_ID INT,
    DRIVER_RATING FLOAT,
    CITY VARCHAR,
    LAST_UPDATED_TIMESTAMP DATE
);

-- Copy data from azure blob storage into the drivers table
COPY INTO logistics_db.raw.drivers
    FROM @logistics_db.raw.logistics_container_stage
    FILES = ('snow_drivers.csv')
    FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Create locations table
CREATE OR REPLACE TABLE logistics_db.raw.locations(
    LOCATION_ID INT,
    CITY VARCHAR,
    STATE VARCHAR,
    COUNTRY VARCHAR,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    LAST_UPDATED_TIMESTAMP DATE
);

-- Copy data from azure blob storage into the locations table
COPY INTO logistics_db.raw.locations
    FROM @logistics_db.raw.logistics_container_stage
    FILES = ('snow_locations.csv')
    FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Create payments table
CREATE OR REPLACE TABLE logistics_db.raw.payments(
    PAYMENT_ID INT,
    TRIP_ID INT,
    CUSTOMER_ID INT,
    PAYMENT_METHOD VARCHAR,
    PAYMENT_STATUS VARCHAR,
    AMOUNT FLOAT,
    TRANSACTION_TIME DATE,
    LAST_UPDATED_TIMESTAMP DATE
);

-- Copy data from azure blob storage into the payments table
COPY INTO logistics_db.raw.payments
    FROM @logistics_db.raw.logistics_container_stage
    FILES = ('snow_payments.csv')
    FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Create trips table
CREATE OR REPLACE TABLE logistics_db.raw.trips(
    TRIP_ID INT,
    DRIVER_ID INT,
    CUSTOMER_ID INT,
    VEHICLE_ID INT,
    TRIP_START_TIME TIMESTAMP,
    TRIP_END_TIME TIMESTAMP,
    START_LOCATION VARCHAR,
    END_LOCATION VARCHAR,
    DISTANCE_KM FLOAT,
    FARE_AMOUNT FLOAT,
    PAYMENT_METHOD VARCHAR,
    TRIP_STATUS VARCHAR,
    LAST_UPDATED_TIMESTAMP TIMESTAMP
);

-- Copy data from azure blob storage into the trips table
COPY INTO logistics_db.raw.trips
    FROM @logistics_db.raw.logistics_container_stage
    FILES = ('snow_trips.csv')
    FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Create vehicles table
CREATE OR REPLACE TABLE logistics_db.raw.vehicles(
    VEHICLE_ID INT,
    LICENSE_PLATE VARCHAR,
    MODEL VARCHAR,
    MAKE VARCHAR,
    YEAR INT,
    VEHICLE_TYPE VARCHAR,
    LAST_UPDATED_TIMESTAMP TIMESTAMP
);

-- Copy data from azure blob storage into the vehicles table
COPY INTO logistics_db.raw.vehicles
    FROM @logistics_db.raw.logistics_container_stage
    FILES = ('snow_vehicles.csv')
    FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

select * from logistics_db.raw.vehicles
