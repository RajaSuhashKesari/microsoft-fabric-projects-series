You can  create queries in datflow gen2 by using below codes

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
