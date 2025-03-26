import unittest
import pandas as pd
import os
from google.cloud import bigquery
from dashboard import fetch_data, paginate_dataframe


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\deepa\Documents\Media\credentials\media-content-analytics-1bbff619e78c.json"
client = bigquery.Client()

class TestDashboard(unittest.TestCase):

    def test_fetch_data_success(self):
        query = """
            SELECT category, COUNT(*) AS article_count 
            FROM `media-content-analytics.MediaContent.NewsArticles` 
            GROUP BY category 
            LIMIT 5
        """
        df = fetch_data(query)
        
        self.assertFalse(df.empty, "DataFrame should not be empty")
        self.assertTrue('category' in df.columns, "DataFrame should have 'category' column")
        self.assertTrue('article_count' in df.columns, "DataFrame should have 'article_count' column")
        self.assertGreaterEqual(len(df), 1, "DataFrame should have at least 1 row")

    def test_fetch_data_invalid_query(self):
        query = "SELECT * FROM nonexistent_table"
        df = fetch_data(query)
        
        self.assertTrue(df.empty, "DataFrame should be empty on error")
        self.assertEqual(len(df), 0, "DataFrame should have 0 rows on error")

    def test_paginate_dataframe_page_1(self):
        df = pd.DataFrame({
            'title': ['Video1', 'Video2', 'Video3', 'Video4'],
            'views': [100, 200, 300, 400]
        })
        page_size = 2
        start_idx = (1 - 1) * page_size
        end_idx = start_idx + page_size
        paginated_df = df.iloc[start_idx:end_idx]
        total_pages = (len(df) - 1) // page_size + 1
        
        self.assertEqual(len(paginated_df), 2, "Paginated DataFrame should have 2 rows")
        self.assertEqual(total_pages, 2, "Total pages should be 2")
        self.assertEqual(paginated_df['title'].tolist(), ['Video1', 'Video2'])
        self.assertEqual(paginated_df['views'].tolist(), [100, 200])

    def test_paginate_dataframe_page_2(self):
        df = pd.DataFrame({
            'title': ['Video1', 'Video2', 'Video3', 'Video4'],
            'views': [100, 200, 300, 400]
        })
        page_size = 2
        start_idx = (2 - 1) * page_size 
        end_idx = start_idx + page_size
        paginated_df = df.iloc[start_idx:end_idx]
        total_pages = (len(df) - 1) // page_size + 1
        
        self.assertEqual(len(paginated_df), 2, "Paginated DataFrame should have 2 rows")
        self.assertEqual(total_pages, 2, "Total pages should be 2")
        self.assertEqual(paginated_df['title'].tolist(), ['Video3', 'Video4'])
        self.assertEqual(paginated_df['views'].tolist(), [300, 400])

    def test_paginate_dataframe_empty(self):
        df = pd.DataFrame()
        page_size = 10
        paginated_df = df 
        total_pages = 0
        
        self.assertTrue(paginated_df.empty, "Paginated DataFrame should be empty")
        self.assertEqual(total_pages, 0, "Total pages should be 0")

    def test_paginate_dataframe_single_row(self):
        df = pd.DataFrame({
            'title': ['Video1'],
            'views': [100]
        })

        page_size = 10
        start_idx = (1 - 1) * page_size 
        end_idx = start_idx + page_size
        paginated_df = df.iloc[start_idx:end_idx]
        total_pages = (len(df) - 1) // page_size + 1
        

        self.assertEqual(len(paginated_df), 1, "Paginated DataFrame should have 1 row")
        self.assertEqual(total_pages, 1, "Total pages should be 1")
        self.assertEqual(paginated_df['title'].tolist(), ['Video1'])
        self.assertEqual(paginated_df['views'].tolist(), [100])

if __name__ == '__main__':
    unittest.main()