from google.cloud import bigquery
import pandas as pd
import os
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\deepa\Documents\Media\credentials\media-content-analytics-1bbff619e78c.json"

client = bigquery.Client()
json_file = r"C:\Users\deepa\Documents\Media\Data\news.json"

with open(json_file, "r") as f:
    data = [json.loads(line) for line in f]
    
df = pd.DataFrame(data)

df = df.head(20000)

print("Sample Data:\n", df.head())

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("headline", "STRING"),
        bigquery.SchemaField("authors", "STRING"),
        bigquery.SchemaField("link", "STRING"),
        bigquery.SchemaField("short_description", "STRING"),
        bigquery.SchemaField("date", "STRING"), 
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
)

dataset_id = "MediaContent"
table_id = "NewsArticles"
table_ref = client.dataset(dataset_id).table(table_id)

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result() 

print(f"Successfully uploaded {job.output_rows} rows to BigQuery table `{dataset_id}.{table_id}`")

query = f"SELECT * FROM `{client.project}.{dataset_id}.{table_id}` LIMIT 10"
query_job = client.query(query)


result_df = query_job.to_dataframe()


print("\n Queried Data from BigQuery:")
print(result_df)