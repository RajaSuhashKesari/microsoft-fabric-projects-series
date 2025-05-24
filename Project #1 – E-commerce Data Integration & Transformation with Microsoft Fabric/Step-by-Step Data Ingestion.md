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
