from google.cloud import bigquery
import pandas as pd

# Set up BigQuery client (ensure you have authentication set up)
client = bigquery.Client()

# Read your SQL query from feature_view.sql
with open("feature_view.sql", "r") as f:
    query = f.read()

# Run the query and load results into a Pandas DataFrame
df = client.query(query).to_dataframe()

# Save the raw data as CSV (optional)
df.to_csv("data/raw/raw_signups.csv", index=False)