import os
import requests
import pandas as pd
import time
from google.cloud import bigquery


API_KEY = "Your Youtube API Key"


CHANNELS = {
    "ABN Telugu": "UC_2irx_BQR7RsBKmUV9fePQ",
    "ETV Andhra Pradesh": "UCJi8M0hRKjz8SLPvJKEVTOg",
    "sumantvtelugulive": "UC4WoZ4p4sg-rG-EdMXGmgTg",
    "adityamusic": "UCNApqoVYJbYSrni4YsbXzyQ",
    "T-Series Telugu": "UCnJjcn5FrgrOEp5_N45ZLEQ",
    "SaregamaMusic": "UCdPc54sruYz59yz9iYEHFYQ",
    "Prasadtechintelugu": "UCb-xXZ7ltTvrh9C6DgB9H-Q",
    "VaasuTechVlogs": "UCv-rv0NzHp-DSRkoxo5TXCA",
    "shravaniskitchen": "UCvLj42lkMc6SlWcKXFFVu6w",
    "AmmaChethiVanta": "UCP2JIsLWvpPoS82e49YAAlw"
}

def fetch_category_mapping():
    url = "https://www.googleapis.com/youtube/v3/videoCategories"
    params = {"part": "snippet", "regionCode": "IN", "key": API_KEY}
    response = requests.get(url, params=params).json()

    return {item["id"]: item["snippet"]["title"] for item in response.get("items", [])}


def get_channel_statistics(channel_id):
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {"part": "statistics", "id": channel_id, "key": API_KEY}
    response = requests.get(url, params=params).json()

    if "items" in response:
        stats = response["items"][0]["statistics"]
        return {"subscribers_count": stats.get("subscriberCount", 0), "total_videos": stats.get("videoCount", 0)}
    
    return {"subscribers_count": 0, "total_videos": 0}


def get_channel_videos(channel_id):
    base_url = "https://www.googleapis.com/youtube/v3/search"
    video_ids, next_page_token = [], None

    while len(video_ids) < 10000:
        params = {
            "part": "id", "channelId": channel_id, "maxResults": 50,
            "order": "date", "type": "video", "pageToken": next_page_token, "key": API_KEY
        }
        response = requests.get(base_url, params=params).json()
        video_ids.extend([item["id"]["videoId"] for item in response.get("items", [])])
        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break
        time.sleep(1)

    return video_ids[:10000]


def get_video_details(video_ids, category_mapping):
    video_url = "https://www.googleapis.com/youtube/v3/videos"
    videos = []

    for i in range(0, len(video_ids), 50):
        params = {"part": "snippet,statistics", "id": ",".join(video_ids[i:i+50]), "key": API_KEY}
        response = requests.get(video_url, params=params).json()

        for item in response.get("items", []):
            snippet, stats = item["snippet"], item.get("statistics", {})
            category_name = category_mapping.get(snippet.get("categoryId", ""), "Unknown")

            videos.append({
                "video_id": item["id"],
                "title": snippet["title"],
                "channel_title": snippet["channelTitle"],
                "category_id": snippet.get("categoryId", ""),
                "category_name": category_name,
                "publish_time": snippet["publishedAt"],
                "description": snippet.get("description", ""),
                "tags": ", ".join(snippet.get("tags", [])) if snippet.get("tags") else "",
                "thumbnail_link": snippet["thumbnails"]["high"]["url"],
                "video_link": f"https://www.youtube.com/watch?v={item['id']}",
                "views": stats.get("viewCount", 0),
                "likes": stats.get("likeCount", 0),
                "comment_count": stats.get("commentCount", 0),
                "comments_disabled": "commentCount" not in stats
            })
        time.sleep(1)
    return videos


def get_all_channel_data():
    all_videos = []
    category_mapping = fetch_category_mapping()

    for channel_name, channel_id in CHANNELS.items():
        print(f"Fetching data for {channel_name}...")

        channel_stats = get_channel_statistics(channel_id)
        video_ids = get_channel_videos(channel_id)

        if not video_ids:
            print(f"No videos found for {channel_name}")
            continue

        videos = get_video_details(video_ids, category_mapping)
        for video in videos:
            video.update({"channel_name": channel_name, "subscribers_count": channel_stats["subscribers_count"], "total_videos": channel_stats["total_videos"]})

        all_videos.extend(videos)

    return pd.DataFrame(all_videos)

 
project_id = "media-content-analytics"
dataset_id = "MediaContent"
table_id = "youtube_data"

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)

    numeric_columns = ["views", "likes", "comment_count", "subscribers_count", "total_videos"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)  

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
        autodetect=True, 
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print(f" Data uploaded to BigQuery table {dataset_id}.{table_id} successfully!")


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\deepa\Documents\Media\credentials\media-content-analytics-1bbff619e78c.json"


df = get_all_channel_data()

if not df.empty:
    upload_to_bigquery(df, project_id, dataset_id, table_id)
else:
    print(" No data fetched. Please check your API key or channel IDs.")
