# Fabric notebook source


# MARKDOWN ********************

# # Manage the semantic model lifecycle
# 
# This notebook generates sample data in the attached lakehouse and validates a semantic model using SemPy.

# MARKDOWN ********************

# ## Generate sample data
# 
# The following cell creates three Delta tables in the attached lakehouse: **products** (10 rows), **customers** (20 rows), and **sales** (200 rows). The sales table includes intentional data quality issues Ã¢â‚¬â€ three null `CustomerKey` values and 10 rows with an orphaned `CustomerKey` of 99 Ã¢â‚¬â€ so you can detect them during validation.

# CELL ********************

import pandas as pd
from datetime import date, timedelta
import random

random.seed(42)

# Products table (10 products across 3 categories)
products = pd.DataFrame({
    "ProductKey": range(1, 11),
    "ProductName": [
        "Mountain Bike", "Road Bike", "Touring Bike", "Helmet", "Gloves",
        "Jersey", "Shorts", "Water Bottle", "Tire Pump", "Bike Light"
    ],
    "Category": [
        "Bikes", "Bikes", "Bikes", "Accessories", "Clothing",
        "Clothing", "Clothing", "Accessories", "Accessories", "Accessories"
    ],
    "UnitCost": [1200.00, 900.00, 800.00, 35.00, 25.00,
                 55.00, 40.00, 12.00, 20.00, 30.00]
})

# Customers table (20 customers across 10 cities)
customers = pd.DataFrame({
    "CustomerKey": range(1, 21),
    "CustomerName": [f"Customer {i}" for i in range(1, 21)],
    "City": [
        "Seattle", "Portland", "San Francisco", "Los Angeles", "Denver",
        "Chicago", "New York", "Boston", "Atlanta", "Dallas",
        "Seattle", "Portland", "San Francisco", "Los Angeles", "Denver",
        "Chicago", "New York", "Boston", "Atlanta", "Dallas"
    ],
    "State": [
        "WA", "OR", "CA", "CA", "CO", "IL", "NY", "MA", "GA", "TX",
        "WA", "OR", "CA", "CA", "CO", "IL", "NY", "MA", "GA", "TX"
    ]
})

# Sales table (200 rows with intentional data quality issues)
sales_rows = []
for i in range(1, 201):
    cust_key = 99 if i % 20 == 0 else random.randint(1, 20)
    prod_key = random.randint(1, 10)
    qty = random.randint(1, 8)
    price = round(products.loc[prod_key - 1, "UnitCost"] * random.uniform(1.3, 1.8), 2)
    order_date = date(2025, 1, 1) + timedelta(days=random.randint(0, 364))

    sales_rows.append({
        "SalesKey": i,
        "OrderDate": str(order_date),
        "CustomerKey": cust_key,
        "ProductKey": prod_key,
        "Quantity": qty,
        "UnitPrice": price,
        "Sales": round(qty * price, 2)
    })

sales = pd.DataFrame(sales_rows)

# Insert null CustomerKey values for data quality testing
sales.loc[4, "CustomerKey"] = None
sales.loc[14, "CustomerKey"] = None
sales.loc[24, "CustomerKey"] = None

# Convert CustomerKey to nullable integer to preserve type consistency
sales["CustomerKey"] = sales["CustomerKey"].astype(pd.Int64Dtype())

# Save as Delta tables in the lakehouse
spark.createDataFrame(products).write.format("delta").mode("overwrite").saveAsTable("products")
spark.createDataFrame(customers).write.format("delta").mode("overwrite").saveAsTable("customers")
spark.createDataFrame(sales).write.format("delta").mode("overwrite").saveAsTable("sales")

print("Created tables: products (10 rows), customers (20 rows), sales (200 rows)")
print("Data includes: 3 null CustomerKey values, 10 orphaned CustomerKey values (99)")

# MARKDOWN ********************

# ## Validate the semantic model with SemPy
# 
# Before running the cells below, create a semantic model named **SalesData** from the lakehouse tables (products, customers, sales), then return to this notebook.
# 
# SemPy connects to the semantic model through the XMLA endpoint. The following cells inspect model metadata, check data quality, discover relationships, and evaluate a DAX query.

# CELL ********************

import sempy.fabric as fabric

# List all tables in the SalesData semantic model to confirm it's accessible
tables = fabric.list_tables("SalesData")
display(tables)

# CELL ********************

# List every column across all tables, showing name, data type, and parent table
# This helps you understand an unfamiliar model without opening Power BI Desktop
columns = fabric.list_columns("SalesData")
display(columns)

# CELL ********************

# Read the sales table from the semantic model into a pandas DataFrame
sales_df = fabric.read_table("SalesData", "sales")

# Check for null values across all columns Ã¢â‚¬â€ nulls in foreign key columns
# mean rows can't join to dimension tables, causing blanks in reports
null_counts = sales_df.isnull().sum()
print("Null values per column:")
print(null_counts[null_counts > 0])

# Check for duplicate primary keys Ã¢â‚¬â€ duplicates would inflate aggregations
duplicate_keys = sales_df["SalesKey"].duplicated().sum()
print(f"\nDuplicate SalesKey values: {duplicate_keys}")
print(f"Total rows: {len(sales_df)}")

# CELL ********************

from sempy.relationships import find_relationships, list_relationship_violations

# Read the dimension tables from the semantic model
products_df = fabric.read_table("SalesData", "products")
customers_df = fabric.read_table("SalesData", "customers")

# SemPy discovers relationships by matching column name patterns (like "Key"
# suffixes) and checking value overlap between tables
all_tables = [sales_df, products_df, customers_df]
relationships = find_relationships(all_tables)
display(relationships)

# CELL ********************

# Check for orphaned foreign keys Ã¢â‚¬â€ values in the fact table that have no
# matching row in the dimension table. These cause blank rows in reports.
violations = list_relationship_violations(all_tables, relationships)
display(violations)

# CELL ********************

# Evaluate a DAX query to check if the semantic model returns correct results
# Without relationships, the engine can't link sales to products Ã¢â‚¬â€ every
# category shows the same totals because the filter context doesn't propagate
result = fabric.evaluate_dax(
    "SalesData",
    """
    EVALUATE
    SUMMARIZECOLUMNS(
        'products'[Category],
        "Total Sales", SUM('sales'[Sales]),
        "Total Quantity", SUM('sales'[Quantity])
    )
    ORDER BY [Total Sales] DESC
    """
)
display(result)

# MARKDOWN ********************

# ## Fix the semantic model with SemPy
# 
# The DAX results show identical totals for every category because the semantic model has no relationships defined. Without relationships, the DAX engine can't filter sales by product category.
# 
# The `connect_semantic_model` function opens a read/write connection to the semantic model's Tabular Object Model (TOM). The following cells use TOM to create the missing many-to-one relationships, then re-run the DAX query to confirm the fix.

# CELL ********************

import Microsoft.AnalysisServices.Tabular as TOM

# Open a read/write connection to the semantic model's TOM
with fabric.connect_semantic_model("SalesData", readonly=False) as tom:

    # Create sales[ProductKey] -> products[ProductKey] (many-to-one)
    rel_product = TOM.SingleColumnRelationship()
    rel_product.Name = "sales_to_products"
    rel_product.FromColumn = tom.model.Tables["sales"].Columns["ProductKey"]
    rel_product.ToColumn = tom.model.Tables["products"].Columns["ProductKey"]
    rel_product.FromCardinality = TOM.RelationshipEndCardinality.Many
    rel_product.ToCardinality = TOM.RelationshipEndCardinality.One
    tom.model.Relationships.Add(rel_product)

    # Create sales[CustomerKey] -> customers[CustomerKey] (many-to-one)
    rel_customer = TOM.SingleColumnRelationship()
    rel_customer.Name = "sales_to_customers"
    rel_customer.FromColumn = tom.model.Tables["sales"].Columns["CustomerKey"]
    rel_customer.ToColumn = tom.model.Tables["customers"].Columns["CustomerKey"]
    rel_customer.FromCardinality = TOM.RelationshipEndCardinality.Many
    rel_customer.ToCardinality = TOM.RelationshipEndCardinality.One
    tom.model.Relationships.Add(rel_customer)

# Changes save automatically when the context manager exits
print("Added 2 relationships to the SalesData semantic model:")
print("  sales[ProductKey] -> products[ProductKey] (many-to-one)")
print("  sales[CustomerKey] -> customers[CustomerKey] (many-to-one)")

# Refresh the semantic model so the new relationships hold data
fabric.refresh_dataset("SalesData")
print("Semantic model refreshed - relationships are now active.")

# CELL ********************

# Re-run the same DAX query â€” now that relationships exist, the engine
# filters sales by product category and returns correct per-category totals
result_fixed = fabric.evaluate_dax(
    "SalesData",
    """
    EVALUATE
    SUMMARIZECOLUMNS(
        'products'[Category],
        "Total Sales", SUM('sales'[Sales]),
        "Total Quantity", SUM('sales'[Quantity])
    )
    ORDER BY [Total Sales] DESC
    """
)
display(result_fixed)
