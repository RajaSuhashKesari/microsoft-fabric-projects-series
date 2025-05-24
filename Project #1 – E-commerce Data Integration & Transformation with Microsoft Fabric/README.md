# 📦 Project #1 – E-commerce Data Integration & Transformation with Microsoft Fabric

## 📌 Project Overview

This project demonstrates an end-to-end data engineering pipeline built using **Microsoft Fabric**. The focus is on ingesting and transforming Brazil's largest e-commerce dataset (from [Olist on GitHub](https://github.com/mayank953/BigDataProjects/tree/main/Project-Brazillian%20Ecommerce/Data)) and storing the final cleansed data in **Lakehouse Delta Tables** for analytics use cases.

## 🔧 Tools & Technologies Used

- Microsoft Fabric
  - Dataflows Gen2
  - Lakehouse
  - Power Query
- GitHub (for source dataset)
- Delta Tables
- Data Engineering Concepts

## 📥 Data Ingestion

Multiple CSV files were ingested from the GitHub Olist dataset using **M Query** inside Dataflows Gen2.

## 🧹 Data Transformations & Joins

The following key transformations were performed:

- Removed duplicate records in the `Orders` table.
- Converted timestamp columns (e.g., `order_purchase_timestamp`) to **Date** type.
- Added a new column `estimated_delivery_time` by subtracting `order_purchase_date` from `order_estimated_delivery_date`.
- Created a `Delay` flag using conditional logic comparing `actual_delivery_time` vs. `estimated_delivery_time`.
- Created a `No_days_delay` column with calculated delay in days.
- Performed **left outer joins** across:
  - `Orders` with `Customers`
  - `Orders` with `Payments`
  - `Orders` with `OrderItems`
  - `Products`, `Sellers`, and `Product Category Translations`

## 🗃️ Load Data to Storage (Lakehouse)

Transformed data was stored in Delta Tables in Microsoft Fabric Lakehouse. These tables are ready for querying in the SQL Endpoint and can be visualized via Power BI or used in ML models.

## 📈 Outcome

A clean and enriched ecommerce dataset, ready for further analysis such as:

- Delivery performance
- Payment trends
- Sales breakdown by location
- Product category insights

---

## 📦 Result

Each dataset is transformed and saved as an individual query in Dataflows Gen2, ready for further modeling or loading into a Microsoft Fabric **Lakehouse** as Delta tables.

This method provides a modular, scalable, and professional approach to managing data ingestion pipelines from GitHub sources.

## 📌 Author

Kesari Raja Suhash   
[LinkedIn Profile](https://www.linkedin.com/in/RajaSuhashKesari)
Aspiring Data Engineer | Microsoft Fabric Enthusiast 
