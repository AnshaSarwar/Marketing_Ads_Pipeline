import numpy as np
from datetime import datetime
import logging
import pandas as pd

# Set up logging
logger = logging.getLogger(__name__)

# Function to generate company data
def generate_company_data():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:00')
    company_data = []

    for id_ in range(1, 6):
        clicks = np.random.randint(10, 500)
        company_data.append({
            'id': id_,
            'created_at': timestamp,
            'impressions': np.random.randint(500, 5000),
            'clicks': clicks,
            'spending': np.random.uniform(100, 1000),
            'conversions': max(int(clicks * np.random.uniform(0.1, 0.2)), 1),
            'Ad_Type': np.random.choice(["text", "banner", "video"]),
            'Target_Audience_Demographics': np.random.choice(["18-24, Male", "25-34, Female", "35-44, All", "45+, Female"]),
            'Platform_Type': np.random.choice(["search", "display network", "social media"]),
            'Campaign_Goal': np.random.choice(["awareness", "conversion", "engagement"]),
            'Geographic_Targeting': np.random.choice(["North America", "Europe", "Asia-Pacific", "Global"])
        })
    return company_data

# Function to generate Google data
def generate_google_data():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:00')
    google_data = []

    for id_ in range(1, 6):
        clicks = np.random.randint(20, 1000)
        google_data.append({
            'id': id_,
            'created_at': timestamp,
            'impressions': np.random.randint(1000, 10000),
            'clicks': clicks,
            'revenue': np.random.uniform(100, 5000),
            'conversions': max(int(clicks * np.random.uniform(0.1, 0.2)), 1),
            'Ad_Format': np.random.choice(["responsive", "static", "interactive"]),
            'Advertiser_Industry': np.random.choice(["tech", "healthcare", "retail", "education"]),
            'Device_Targeting': np.random.choice(["mobile", "desktop", "tablet", "all"]),
            'Keyword_Targeting': np.random.choice(["tech gadgets", "fitness", "online courses", "fashion"]),
            'Ad_Auction_Rank': np.random.randint(1, 10)
        })
    return google_data