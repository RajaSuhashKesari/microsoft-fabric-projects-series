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

# 📥 Data Ingestion Process – Microsoft Fabric (Dataflows Gen2)

This guide explains how we ingested multiple CSV datasets from a GitHub repository into Microsoft Fabric Dataflows Gen2 using a reusable helper function in Power Query (M language).

---

## 🧠 Step-by-Step Breakdown

### 1. **Create a Blank Query**

* In Microsoft Fabric’s **Dataflows Gen2**, create a new **Blank Query**.

### 2. **Write a Helper Function to Load GitHub CSV Files**

A reusable function `LoadCSV(fileName)` is defined to load CSVs from GitHub:

```m
LoadCSV = (fileName as text) =>
    Table.PromoteHeaders(
        Csv.Document(
            Web.Contents(
                "https://raw.githubusercontent.com",
                [ RelativePath = "mayank953/BigDataProjects/refs/heads/main/Project-Brazillian Ecommerce/Data/" & fileName ]
            ),
            [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
        ),
        [PromoteAllScalars = true]
    );
```

### 3. **Load All Required Tables Using This Function**

Each dataset is loaded by calling the `LoadCSV` function with the corresponding file name:

```m
Customers = LoadCSV("olist_customers_dataset.csv"),
GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
OrderItems = LoadCSV("olist_order_items_dataset.csv"),
Payments = LoadCSV("olist_order_payments_dataset.csv"),
Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
Orders = LoadCSV("olist_orders_dataset.csv"),
Products = LoadCSV("olist_products_dataset.csv"),
Sellers = LoadCSV("olist_sellers_dataset.csv"),
CategoryTranslation = LoadCSV("product_category_name_translation.csv"),

Output = [
    Customers = Customers,
    GeoLocation = GeoLocation,
    OrderItems = OrderItems,
    Payments = Payments,
    Reviews = Reviews,
    Orders = Orders,
    Products = Products,
    Sellers = Sellers,
    CategoryTranslation = CategoryTranslation
]
```

### 4. **Drill Down to Individual Tables**

* After running the query, a record with multiple fields (one for each dataset) is returned.
* For each field:

  * Right-click the field and select **Drill Down**.
  * Rename the resulting query (e.g., "Orders", "Payments").
  * Apply necessary transformations like:

    * Column renaming
    * Data type conversions
    * Adding calculated columns

---

## ✅ Benefits of This Approach

* **Efficient**: Centralized ingestion logic using a single function.
* **Clean**: Eliminates code repetition across queries.
* **Dynamic**: Easily adjustable by just modifying filenames.
* **GitHub-Compatible**: Works well with GitHub raw file URLs when folder ingestion is not supported.

---

## 📦 Result

Each dataset is transformed and saved as an individual query in Dataflows Gen2, ready for further modeling or loading into a Microsoft Fabric **Lakehouse** as Delta tables.

This method provides a modular, scalable, and professional approach to managing data ingestion pipelines from GitHub sources.

## 📌 Author

Kesari Raja Suhash   
[LinkedIn Profile](https://www.linkedin.com/in/RajaSuhashKesari)
Aspiring Data Engineer | Microsoft Fabric Enthusiast 
