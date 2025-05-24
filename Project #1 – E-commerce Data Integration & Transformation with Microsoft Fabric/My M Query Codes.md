You can  create queries in datflow gen2 by using below codes

# Orders
```m
let
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
        ),
  Customers = LoadCSV("olist_customers_dataset.csv"),
  GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
  OrderItems = LoadCSV("olist_order_items_dataset.csv"),
  Payments = LoadCSV("olist_order_payments_dataset.csv"),
  Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
  Orders = LoadCSV("olist_orders_dataset.csv"),
  Products = LoadCSV("olist_products_dataset.csv"),
  Sellers = LoadCSV("olist_sellers_dataset.csv"),
  CategoryTranslation = LoadCSV("product_category_name_translation.csv"),
  Custom = [
        Customers = Customers,
        GeoLocation = GeoLocation,
        OrderItems = OrderItems,
        Payments = Payments,
        Reviews = Reviews,
        Orders = Orders,
        Products = Products,
        Sellers = Sellers,
        CategoryTranslation = CategoryTranslation
    ],
  #"Drill down" = Custom[Orders],
  #"Removed duplicates" = Table.Distinct(#"Drill down", {"order_id"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed duplicates", {{"order_id", type text}, {"customer_id", type text}, {"order_status", type text}, {"order_purchase_timestamp", type datetime}, {"order_approved_at", type datetime}, {"order_delivered_carrier_date", type datetime}, {"order_delivered_customer_date", type datetime}, {"order_estimated_delivery_date", type datetime}}),
  #"Removed blank rows" = Table.SelectRows(#"Changed column type", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed blank rows", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Duplicated column" = Table.DuplicateColumn(#"Removed blank rows 1", "order_purchase_timestamp", "order_purchase_timestamp - Copy"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Duplicated column", {{"order_purchase_timestamp - Copy", type date}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"order_purchase_timestamp - Copy", "order_purchase_date"}}),
  #"Duplicated column 1" = Table.DuplicateColumn(#"Renamed columns", "order_delivered_customer_date", "order_delivered_customer_date - Copy"),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Duplicated column 1", {{"order_delivered_customer_date - Copy", type date}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 2", {{"order_delivered_customer_date", "order_delivered_customer_timestamp"}, {"order_delivered_customer_date - Copy", "order_delivered_customer_date"}, {"order_estimated_delivery_date", "order_estimated_delivery_date"}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Renamed columns 1", {{"order_estimated_delivery_date", type date}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 3", "actual_delivery_time", each [order_delivered_customer_date] - [order_purchase_date]),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "estimated_delivery_time", each [order_estimated_delivery_date] - [order_purchase_date]),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Added custom 1", {{"actual_delivery_time", Int64.Type}, {"estimated_delivery_time", Int64.Type}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type 4","Delay",
    each if [actual_delivery_time] <> null and [estimated_delivery_time] <> null and [actual_delivery_time] > [estimated_delivery_time] then "Yes" else "No"
),

#"Added custom 3" = Table.AddColumn(
    #"Added custom 2",
    "No_days_delay",
    each if [Delay] = "Yes" and [actual_delivery_time] <> null and [estimated_delivery_time] <> null then
        [actual_delivery_time] - [estimated_delivery_time]
    else
        0
)

in
  #"Added custom 3"
```


# Customers
```m
let
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
        ),
  Customers = LoadCSV("olist_customers_dataset.csv"),
  GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
  OrderItems = LoadCSV("olist_order_items_dataset.csv"),
  Payments = LoadCSV("olist_order_payments_dataset.csv"),
  Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
  Orders = LoadCSV("olist_orders_dataset.csv"),
  Products = LoadCSV("olist_products_dataset.csv"),
  Sellers = LoadCSV("olist_sellers_dataset.csv"),
  CategoryTranslation = LoadCSV("product_category_name_translation.csv"),
  Custom = [
        Customers = Customers,
        GeoLocation = GeoLocation,
        OrderItems = OrderItems,
        Payments = Payments,
        Reviews = Reviews,
        Orders = Orders,
        Products = Products,
        Sellers = Sellers,
        CategoryTranslation = CategoryTranslation
    ],
  #"Drill down" = Custom[Customers]
in
  #"Drill down"
```

# OrderItems
```m
let
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
        ),
  Customers = LoadCSV("olist_customers_dataset.csv"),
  GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
  OrderItems = LoadCSV("olist_order_items_dataset.csv"),
  Payments = LoadCSV("olist_order_payments_dataset.csv"),
  Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
  Orders = LoadCSV("olist_orders_dataset.csv"),
  Products = LoadCSV("olist_products_dataset.csv"),
  Sellers = LoadCSV("olist_sellers_dataset.csv"),
  CategoryTranslation = LoadCSV("product_category_name_translation.csv"),
  Custom = [
        Customers = Customers,
        GeoLocation = GeoLocation,
        OrderItems = OrderItems,
        Payments = Payments,
        Reviews = Reviews,
        Orders = Orders,
        Products = Products,
        Sellers = Sellers,
        CategoryTranslation = CategoryTranslation
    ],
  #"Drill down" = Custom[OrderItems]
in
  #"Drill down"
```
# OrderItems
```m
let
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
        ),
  Customers = LoadCSV("olist_customers_dataset.csv"),
  GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
  OrderItems = LoadCSV("olist_order_items_dataset.csv"),
  Payments = LoadCSV("olist_order_payments_dataset.csv"),
  Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
  Orders = LoadCSV("olist_orders_dataset.csv"),
  Products = LoadCSV("olist_products_dataset.csv"),
  Sellers = LoadCSV("olist_sellers_dataset.csv"),
  CategoryTranslation = LoadCSV("product_category_name_translation.csv"),
  Custom = [
        Customers = Customers,
        GeoLocation = GeoLocation,
        OrderItems = OrderItems,
        Payments = Payments,
        Reviews = Reviews,
        Orders = Orders,
        Products = Products,
        Sellers = Sellers,
        CategoryTranslation = CategoryTranslation
    ],
  #"Drill down" = Custom[OrderItems]
in
  #"Drill down"
```
# Payments
```m
let
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
        ),
  Customers = LoadCSV("olist_customers_dataset.csv"),
  GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
  OrderItems = LoadCSV("olist_order_items_dataset.csv"),
  Payments = LoadCSV("olist_order_payments_dataset.csv"),
  Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
  Orders = LoadCSV("olist_orders_dataset.csv"),
  Products = LoadCSV("olist_products_dataset.csv"),
  Sellers = LoadCSV("olist_sellers_dataset.csv"),
  CategoryTranslation = LoadCSV("product_category_name_translation.csv"),
  Custom = [
        Customers = Customers,
        GeoLocation = GeoLocation,
        OrderItems = OrderItems,
        Payments = Payments,
        Reviews = Reviews,
        Orders = Orders,
        Products = Products,
        Sellers = Sellers,
        CategoryTranslation = CategoryTranslation
    ],
  #"Drill down" = Custom[Payments]
in
  #"Drill down"
```
# Products
```m
let
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
        ),
  Customers = LoadCSV("olist_customers_dataset.csv"),
  GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
  OrderItems = LoadCSV("olist_order_items_dataset.csv"),
  Payments = LoadCSV("olist_order_payments_dataset.csv"),
  Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
  Orders = LoadCSV("olist_orders_dataset.csv"),
  Products = LoadCSV("olist_products_dataset.csv"),
  Sellers = LoadCSV("olist_sellers_dataset.csv"),
  CategoryTranslation = LoadCSV("product_category_name_translation.csv"),
  Custom = [
        Customers = Customers,
        GeoLocation = GeoLocation,
        OrderItems = OrderItems,
        Payments = Payments,
        Reviews = Reviews,
        Orders = Orders,
        Products = Products,
        Sellers = Sellers,
        CategoryTranslation = CategoryTranslation
    ],
  #"Drill down" = Custom[Products]
in
  #"Drill down"
```
# Sellers
```m
let
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
        ),
  Customers = LoadCSV("olist_customers_dataset.csv"),
  GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
  OrderItems = LoadCSV("olist_order_items_dataset.csv"),
  Payments = LoadCSV("olist_order_payments_dataset.csv"),
  Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
  Orders = LoadCSV("olist_orders_dataset.csv"),
  Products = LoadCSV("olist_products_dataset.csv"),
  Sellers = LoadCSV("olist_sellers_dataset.csv"),
  CategoryTranslation = LoadCSV("product_category_name_translation.csv"),
  Custom = [
        Customers = Customers,
        GeoLocation = GeoLocation,
        OrderItems = OrderItems,
        Payments = Payments,
        Reviews = Reviews,
        Orders = Orders,
        Products = Products,
        Sellers = Sellers,
        CategoryTranslation = CategoryTranslation
    ],
  #"Drill down" = Custom[Sellers]
in
  #"Drill down"
```
# Product_category_name_translations
```m
let
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
        ),
  Customers = LoadCSV("olist_customers_dataset.csv"),
  GeoLocation = LoadCSV("olist_geolocation_dataset.csv"),
  OrderItems = LoadCSV("olist_order_items_dataset.csv"),
  Payments = LoadCSV("olist_order_payments_dataset.csv"),
  Reviews = LoadCSV("olist_order_reviews_dataset.csv"),
  Orders = LoadCSV("olist_orders_dataset.csv"),
  Products = LoadCSV("olist_products_dataset.csv"),
  Sellers = LoadCSV("olist_sellers_dataset.csv"),
  CategoryTranslation = LoadCSV("product_category_name_translation.csv"),
  Custom = [
        Customers = Customers,
        GeoLocation = GeoLocation,
        OrderItems = OrderItems,
        Payments = Payments,
        Reviews = Reviews,
        Orders = Orders,
        Products = Products,
        Sellers = Sellers,
        CategoryTranslation = CategoryTranslation
    ],
  #"Drill down" = Custom[CategoryTranslation]
in
  #"Drill down"
```
# Final_Order_Data
```m
let
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
in
  #"Expanded Product_category_name_translations"
```
