## Logistics Big Data Analytics Project Featuring Snowflake & DBT.

This project implements a structured, multi-layered analytics pipeline following the Medallion Architecture, with secure data persistence in Snowflake and data transformation and testing using DBT.

#### PROJECT ARCHITECTURE
<div align="center">
  <img src="https://github.com/fredie7/dbt-snowflake-analytics/blob/main/logistics_proj/images/Screenshot%20(5425).png?raw=true />
  <br>
   <sub><b></b> </sub>
</div>

The pipeline begins with the collection of raw business data, including customers, drivers, locations, payments, trips, and vehicles. This data is first staged in Snowflake storage and then ingested into the Bronze layer, which serves as the foundational layer for unprocessed data.

Next, the data moves to the Silver layer, where it undergoes cleaning, transformation, and standardization. This process follows best practices for scalability, query performance, and readability, preparing the data for analytics.

For entities such as customers, the pipeline implements slowly changing dimensions using snapshots to track historical changes while allowing new customers to be added.

The sources.yml file defines the structure and metadata of the remote Snowflake tables used by DBT, while the schema.yml file contains data quality tests and constraints to ensure the reliability and integrity of the transformed data.
