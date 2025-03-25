import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os
from google.cloud import bigquery
import logging

# Setup Logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, "dashboard.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.info("Streamlit Dashboard Started")

try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\deepa\Documents\Media\credentials\media-content-analytics-1bbff619e78c.json"
    client = bigquery.Client()
except Exception as e:
    logging.error(f"Error initializing BigQuery client: {str(e)}")
    st.error("Failed to connect to BigQuery. Please check the authentication credentials.")

st.set_page_config(layout="wide")
st.title("Media Analytics Dashboard")

st.sidebar.header("Select Dataset")
dataset_options = ["News Articles", "YouTube Analytics"]
selected_dataset = st.sidebar.selectbox("Choose a dataset:", dataset_options)

def fetch_data(query):
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Data fetched successfully for query: {query[:100]}...")
        return df
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        st.error(f"Failed to retrieve data. Error: {str(e)}")
        return pd.DataFrame()

def paginate_dataframe(df, page_size):
    if df.empty:
        return df, 0
    total_pages = (len(df) - 1) // page_size + 1
    try:
        page_number = st.sidebar.number_input("Page Number", min_value=1, max_value=total_pages, value=1, step=1)
        logging.info(f"Pagination applied - Page: {page_number}/{total_pages}, Page Size: {page_size}")
        start_idx = (page_number - 1) * page_size
        end_idx = start_idx + page_size
        return df.iloc[start_idx:end_idx], total_pages
    except Exception as e:
        logging.error(f"Error in pagination: {str(e)}")
        st.error("An error occurred while paginating the data.")
        return df, total_pages

# NEWS ARTICLES ANALYSIS
try:
    if selected_dataset == "News Articles":
        st.subheader("News Articles Insights")

        queries = {
            "View Complete News Data": "SELECT * FROM `media-content-analytics.MediaContent.NewsArticles`",
            "News Category Distribution": "SELECT category, COUNT(*) AS article_count FROM `media-content-analytics.MediaContent.NewsArticles` GROUP BY category ORDER BY article_count DESC",
            "Articles Published Over Time": "SELECT EXTRACT(YEAR FROM date) AS year, COUNT(*) AS article_count FROM `media-content-analytics.MediaContent.NewsArticles` GROUP BY year ORDER BY year ASC"
        }

        selected_query = st.sidebar.selectbox("Choose a News Query", list(queries.keys()))
        logging.info(f"User selected query: {selected_query}")

        df = fetch_data(queries[selected_query])
        
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df["year"] = df["date"].dt.year

        if "headline" in df.columns: 
            search_term = st.sidebar.text_input("Search Headlines", "")
            if search_term:
                df = df[df["headline"].str.contains(search_term, case=False, na=False)]
                logging.info(f"Applied headline search filter: '{search_term}'")
            else:
                logging.info("No headline search term provided")

        if selected_query == "News Category Distribution":
            st.sidebar.header("Filters")

            try:
                selected_categories = st.sidebar.multiselect(
                    "Select Categories",
                    df["category"].unique().tolist(),
                    default=df["category"].unique().tolist()[:5]
                )
                if selected_categories:
                    df = df[df["category"].isin(selected_categories)]
                    logging.info(f"Applied category filters: {selected_categories}")
                else:
                    logging.info('No category selected, displaying all categories')
            except Exception as e:
                logging.error(f"Error applying category filter: {str(e)}")
                st.error("Failed to apply category filters.")

        if df.empty:
            st.warning("No articles found for the selected query or filters.")
            logging.warning(f"No data available for query: {selected_query}")
        else:
            page_size = st.sidebar.selectbox("Select page size", [10, 20, 50], index=1)
            paginated_df, total_pages = paginate_dataframe(df, page_size)

            st.dataframe(paginated_df, use_container_width=True)
            st.sidebar.write(f"Total Pages: {total_pages}")

            try:
                if selected_query == "News Category Distribution":
                    st.subheader("News Category Distribution")
                    fig, ax = plt.subplots(figsize=(8, 5))
                    ax.barh(df["category"], df["article_count"], color=plt.cm.Paired.colors[: len(df)])
                    ax.set_title("News Categories")
                    ax.set_xlabel("Count")
                    ax.set_ylabel("Category")
                    plt.gca().invert_yaxis()
                    st.pyplot(fig)

                elif selected_query == "Articles Published Over Time":
                    st.subheader("Articles Published Over Time")
                    fig, ax = plt.subplots(figsize=(8, 5))
                    ax.plot(df["year"], df["article_count"], marker="o", linestyle="-", color="blue")
                    ax.set_title("Articles Over Time")
                    ax.set_xlabel("Year")
                    ax.set_ylabel("Count")
                    st.pyplot(fig)
            except Exception as e:
                logging.error(f"Error generating visualization: {str(e)}")
                st.error("An error occurred while generating the graph.")
except Exception as e:
    logging.error(f"Unexpected error in News Articles section: {str(e)}")
    st.error("An unexpected error occurred in the News Articles section.")

# YOUTUBE ANALYTICS
try:
    if selected_dataset == "YouTube Analytics":
        st.subheader("YouTube Data Insights")
        logging.info("User selected: YouTube Analytics")

        queries = {
            "View Complete YouTube Data": "SELECT * FROM `media-content-analytics.MediaContent.youtube_data`",
            "Trending YouTube Videos (Last 7 Days)": """
                SELECT video_id, title, channel_title, views, likes, comment_count, publish_time
                FROM `media-content-analytics.MediaContent.youtube_data`
                WHERE TIMESTAMP(publish_time) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                ORDER BY views DESC
                LIMIT 10
            """,
            "Top Trending YouTube Categories": """
                SELECT category_name, COUNT(video_id) AS total_videos, SUM(views) AS total_views
                FROM `media-content-analytics.MediaContent.youtube_data`
                GROUP BY category_name
                ORDER BY total_views DESC
                LIMIT 10
            """,
            "Most Liked YouTube Videos (All Time)": """
                SELECT video_id, category_name, title, channel_title, likes
                FROM `media-content-analytics.MediaContent.youtube_data`
                ORDER BY likes DESC
                LIMIT 10
            """,

            "Daily Views Trend": """
                SELECT DATE(publish_time) AS publish_date, SUM(views) AS total_views
                FROM `media-content-analytics.MediaContent.youtube_data`
                WHERE publish_time IS NOT NULL
                GROUP BY publish_date
                ORDER BY publish_date DESC
                LIMIT 10
            """
        }

        selected_query = st.sidebar.selectbox("Choose a YouTube Query", list(queries.keys()))
        logging.info(f"User selected query: {selected_query}")

        df = fetch_data(queries[selected_query])

        if df.empty:
            st.warning("No data available for this query.")
            logging.warning(f"No data available for query: {selected_query}")
        else:
            page_size = st.sidebar.selectbox("Select page size", [10, 20, 50], index=1)
            paginated_df, total_pages = paginate_dataframe(df, page_size)

            st.dataframe(paginated_df, use_container_width=True)
            st.sidebar.write(f"Total Pages: {total_pages}")
        try:
            if selected_query != "View Complete YouTube Data":
                fig, ax = plt.subplots(figsize=(8, 5))
                if selected_query == "Trending YouTube Videos (Last 7 Days)":
                    ax.barh(paginated_df["title"], paginated_df["views"], color="skyblue")
                    ax.set_title("Top Trending YouTube Videos")
                    ax.set_xlabel("Views")
                    ax.set_ylabel("Video Title")
                    plt.gca().invert_yaxis()

                elif selected_query == "Top Trending YouTube Categories":
                    fig, ax = plt.subplots(figsize=(8, 5))

                    sorted_df = paginated_df.sort_values(by="total_views", ascending=False)
                    ax.plot(sorted_df["category_name"], sorted_df["total_views"], marker="o", linestyle="-", color="b")
                    ax.set_title("YouTube Categories with Most Views", fontsize=12)
                    ax.set_xlabel("Category Name")
                    ax.set_ylabel("Total Views")

                elif selected_query == "Most Liked YouTube Videos (All Time)":
                    ax.barh(paginated_df["title"], paginated_df["likes"], color="coral")
                    ax.set_title("Most Liked YouTube Videos")
                    ax.set_xlabel("Likes")
                    ax.set_ylabel("Video Title")
                    plt.gca().invert_yaxis()

                elif selected_query == "Daily Views Trend":
                    fig, ax = plt.subplots(figsize=(8, 5))
                    df["publish_date"] = pd.to_datetime(df["publish_date"])
                    ax.plot(df["publish_date"], df["total_views"], marker="o", linestyle="-", color="green")
                    ax.set_title("Total Views Over Time")
                    ax.set_xlabel("Date")
                    ax.set_ylabel("Total Views")
                    plt.xticks(rotation=45)

                st.pyplot(fig)
        except Exception as e:
            logging.error(f"Error generating visualization: {str(e)}")
            st.error("An error occurred while generating the graph.")

except Exception as e:
    logging.error(f"Unexpected error in YouTube Analytics section: {str(e)}")
    st.error("An unexpected error occurred in the YouTube Analytics section.")

logging.info("Streamlit Dashboard Loaded Successfully")
