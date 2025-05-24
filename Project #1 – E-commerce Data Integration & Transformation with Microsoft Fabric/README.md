# 📦 Project #1 – E-commerce Data Integration & Transformation with Microsoft Fabric

## 📌 Project Overview

This project demonstrates an end-to-end data engineering pipeline built using **Microsoft Fabric**. The focus is on ingesting and transforming Brazil's largest e-commerce dataset (from [Olist on GitHub](https://github.com/olist/datasets)) and storing the final cleansed data in **Lakehouse Delta Tables** for analytics use cases.

## 🔧 Tools & Technologies Used

- Microsoft Fabric
  - Dataflows Gen2
  - Lakehouse
  - Power Query (M Language)
- GitHub (for source dataset)
- Delta Tables
- M Language (Power Query)
- Data Engineering Concepts

## 📥 Data Ingestion

Multiple CSV files were ingested from the GitHub Olist dataset using **bulk file ingestion via M Query** inside Dataflows Gen2.

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

### M Code Used for Joins & Expansion:
```m
Source = Table.NestedJoin(Orders, {"customer_id"}, Customers, {"customer_id"}, "Customers", JoinKind.LeftOuter),
#"Expanded Customers" = Table.ExpandTableColumn(Source, "Customers", {"customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"}, {"customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"}),
#"Merged queries" = Table.NestedJoin(#"Expanded Customers", {"order_id"}, Payments, {"order_id"}, "Payments", JoinKind.LeftOuter),
#"Expanded Payments" = Table.ExpandTableColumn(#"Merged queries", "Payments", {"payment_sequential", "payment_type", "payment_installments", "payment_value"}, {"payment_sequential", "payment_type", "payment_installments", "payment_value"}),
#"Merged queries 1" = Table.NestedJoin(#"Expanded Payments", {"order_id"}, OrderItems, {"order_id"}, "OrderItems", JoinKind.LeftOuter),
#"Expanded OrderItems" = Table.ExpandTableColumn(#"Merged queries 1", "OrderItems", {"order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"}, {"order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"}),
#"Merged queries 2" = Table.NestedJoin(#"Expanded OrderItems", {"product_id"}, Products, {"product_id"}, "Products", JoinKind.LeftOuter),
#"Merged queries 3" = Table.NestedJoin(#"Merged queries 2", {"seller_id"}, Sellers, {"seller_id"}, "Sellers", JoinKind.LeftOuter),
#"Expanded Products" = Table.ExpandTableColumn(#"Merged queries 3", "Products", {"product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"}, {"product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"}),
#"Expanded Sellers" = Table.ExpandTableColumn(#"Expanded Products", "Sellers", {"seller_zip_code_prefix", "seller_city", "seller_state"}, {"seller_zip_code_prefix", "seller_city", "seller_state"}),
#"Merged queries 4" = Table.NestedJoin(#"Expanded Sellers", {"product_category_name"}, Product_category_name_translations, {"product_category_name"}, "Product_category_name_translations", JoinKind.LeftOuter),
#"Expanded Product_category_name_translations" = Table.ExpandTableColumn(#"Merged queries 4", "Product_category_name_translations", {"product_category_name_english"}, {"product_category_name_english"})
