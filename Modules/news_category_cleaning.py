from google.cloud import bigquery
import pandas as pd
import os
import re

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\deepa\Documents\Media Content Analytics\media-content-analytics-1bbff619e78c.json"

client = bigquery.Client()

dataset_id = "MediaContent"
table_id = "NewsArticles"
table_ref = client.dataset(dataset_id).table(table_id)


query = f"SELECT * FROM `{table_ref}`"
query_job = client.query(query)
df = query_job.to_dataframe()

print("Raw Data Sample:\n", df.head())


df = df.drop_duplicates()


df["category"] = df["category"].fillna("Unknown")
df["headline"] = df["headline"].fillna("No Headline")
df["authors"] = df["authors"].fillna("Anonymous")
df["short_description"] = df["short_description"].fillna("")
df["link"] = df["link"].fillna("No Link")


df["headline"] = df["headline"].str.strip().str.title()
df["category"] = df["category"].str.strip().str.title()
df["authors"] = df["authors"].str.strip().str.title()
df["short_description"] = df["short_description"].str.strip()


df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date


def is_valid_url(url):
    return bool(re.match(r"https?://[^\s]+", url))

df["link"] = df["link"].apply(lambda x: x if is_valid_url(x) else "Invalid URL")

print("Cleaned Data Sample:\n", df.head())


job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()

print(f"Successfully uploaded cleaned data to `{table_ref}`")
