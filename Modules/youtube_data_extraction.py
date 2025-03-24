import os
import requests
import pandas as pd
import time

API_KEY = "AIzaSyBkpmDbigwB5JOcDaJVVGcv_ywDM6NMrJw"

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
    params = {
        "part": "snippet",
        "regionCode": "IN",
        "key": API_KEY
    }
    response = requests.get(url, params=params).json()
    
    category_mapping = {}
    for item in response.get("items", []):
        category_mapping[item["id"]] = item["snippet"]["title"]
    return category_mapping


def get_channel_statistics(channel_id):
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "statistics",
        "id": channel_id,
        "key": API_KEY
    }
    response = requests.get(url, params=params).json()

    if "items" in response and len(response["items"]) > 0:
        stats = response["items"][0]["statistics"]
        return {
            "subscribers_count": stats.get("subscriberCount", 0),
            "total_videos": stats.get("videoCount", 0),
        }
    return {"subscribers_count": 0, "total_videos": 0}


def get_channel_videos(channel_id):
    base_url = "https://www.googleapis.com/youtube/v3/search"
    video_ids = []
    next_page_token = None

    while len(video_ids) < 10000:
        params = {
            "part": "id",
            "channelId": channel_id,
            "maxResults": 50,
            "order": "date",
            "type": "video",
            "pageToken": next_page_token,
            "key": API_KEY
        }
        response = requests.get(base_url, params=params).json()

        if "items" in response:
            video_ids.extend([item["id"]["videoId"] for item in response["items"]])
        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

        time.sleep(1) 

    return video_ids[:10000] 


def get_video_details(video_ids, category_mapping):
    video_url = "https://www.googleapis.com/youtube/v3/videos"
    videos = []

    for i in range(0, len(video_ids), 50):
        params = {
            "part": "snippet,statistics",
            "id": ",".join(video_ids[i:i+50]),
            "key": API_KEY
        }
        response = requests.get(video_url, params=params).json()

        for item in response.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})

            category_id = snippet.get("categoryId", "")
            category_name = category_mapping.get(category_id, "Unknown")

            videos.append({
                "video_id": item["id"],
                "title": snippet["title"],
                "channel_title": snippet["channelTitle"],
                "category_id": category_id,
                "category_name": category_name,
                "publish_time": snippet["publishedAt"],
                "description": snippet.get("description", ""),
                "tags": ", ".join(snippet.get("tags", [])),
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
            video.update({
                "channel_name": channel_name,
                "subscribers_count": channel_stats["subscribers_count"],
                "total_videos": channel_stats["total_videos"]
            })

        all_videos.extend(videos)

    return pd.DataFrame(all_videos)


df = get_all_channel_data()

if not df.empty:
    file_path = os.path.join("Data", "youtube_videos.csv")
    df.to_csv(file_path, index=False)
    print(f"Data saved to {file_path} (Total Videos Fetched: {len(df)})")
    print(df.head())
else:
    print("No data was fetched. Please check your API key or channel IDs.")
