## Logistics Big Data Analytics Project Featuring Snowflake & DBT.

This project implements a structured, multi-layered analytics pipeline following the Medallion Architecture, with secure data persistence in Snowflake and data transformation and testing using DBT.

#### PROJECT ARCHITECTURE
<div align="center">
  <img src="https://github.com/fredie7/dbt-snowflake-analytics/blob/main/logistics_proj/images/snow.png?raw=true" />
  <br>
   <sub><b></b> </sub>
</div>

The pipeline begins with establishing a connecton between Azure datalakke storage where the data is stored, and snowflake which includes factoring access control between both end points. This raft of data include those of customers, drivers, locations, payments, trips, and vehicles. This data is first staged in Snowflake storage and then ingested into the Bronze layer, which serves as the foundational layer for unprocessed data.tabe and table respectively.

To begin with, the data governance paragym adopted in this project considerd materializing materializing the bronze layer as a view to preserve the storage raw layer. The silver had to be materialized as a table to avoid recomputation which saves compute cost, and also persist data quality upon transformation. The gold layer accounted for a materialized table for fast querying.

Next, the data moves to the Silver layer, where it undergoes cleaning, transformation, and standardization. This process followed best practices for scalability, query performance, and readability, preparing the data for analytics.

For the customer entities, the pipeline implements the slowly changing dimensions - Type 2 using snapshots to track historical change.

The sources.yml file defines the structure and metadata of the remote Snowflake tables used by DBT. It is used to track data lineage in terms of the interdependency beween entities the data model, hence improving documentation

The schema.yml file contains data quality tests and constraints to ensure the reliability and integrity of the transformed data.
