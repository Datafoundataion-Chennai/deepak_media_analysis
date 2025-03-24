import requests

API_KEY = "USE YOUR OWN YOUTUBE API KEY"
USERNAME = "saregamasouth"

url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={USERNAME}&type=channel&key={API_KEY}"

response = requests.get(url).json()
channel_id = response['items'][0]['id']['channelId']

print("Channel ID:", channel_id)
