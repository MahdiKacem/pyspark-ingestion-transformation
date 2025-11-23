# Pyspark ingestion transformation

## Overview

This project implements a Lakehouse-based data pipeline using Databricks, Apache Spark, and dbt.
It follows the Bronze → Silver → Gold architecture and processes streaming or batch CSV data into a clean, analytics-ready Star Schema.

The pipeline is composed of:

**Bronze Layer** 

 - Read streaming or batch CSV sources
 - Write raw data to Bronze Delta tables
   
**Silver Layer**  

 - Deduplication
 - Business logic cleanup
 - Upserts
 - Prepare refined data ready for dbt ingestion written to Silver Delta Tables
   
**Gold Layer**

 - Build a Star Schema (facts + dimensions)
 - Apply modeling best practices (staging → marts → final models)
 - Build snapshots
