from google.cloud import bigquery
import pandas as pd
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\deepa\Documents\Media\credentials\media-content-analytics-1bbff619e78c.json"
project_id = "media-content-analytics"
dataset_id = "MediaContent"
table_id = "youtube_data"
table_ref = f"{project_id}.{dataset_id}.{table_id}"


client = bigquery.Client()


query = f"SELECT * FROM `{table_ref}`"
df = client.query(query).to_dataframe()

print("Before Cleaning:")
print(df.info())
print(df.head())


df["views"] = pd.to_numeric(df["views"], errors="coerce").fillna(0).astype(int)
df["likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(0).astype(int)
df["comment_count"] = pd.to_numeric(df["comment_count"], errors="coerce").fillna(0).astype(int)

df.drop_duplicates(subset=["video_id"], keep="first", inplace=True)
df.dropna(subset=["title", "video_link"], inplace=True)
df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
df = df[df["views"] > 10]
df["tags"].fillna("No Tags", inplace=True)
df["category_name"].fillna("Unknown", inplace=True)


clean_file_path = os.path.join("youtube_videos_cleaned.csv")
df.to_csv(clean_file_path, index=False)

print(f"Data cleaned and saved to {clean_file_path} (Total Videos: {len(df)})")
print(df.head())


job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  
    autodetect=True,  
)

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()  

print(f" Cleaned data uploaded to BigQuery table: {table_ref}")
