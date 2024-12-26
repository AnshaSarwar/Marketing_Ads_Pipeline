import pandas as pd
from datetime import datetime, timedelta
import logging
from data_storage import fetch_data
logger = logging.getLogger(__name__)

def calculate_statistics(company_collection, google_collection):
    try:
        # Fetch the latest data from both collections
        company_df = fetch_data(company_collection)
        google_df = fetch_data(google_collection)

        # Ensure timestamps are in datetime format
        company_df['created_at'] = pd.to_datetime(company_df['created_at'])
        google_df['created_at'] = pd.to_datetime(google_df['created_at'])

        # Create 1-minute intervals for Google data by splitting each 2-minute record
        google_split_data = []

        for index, row in google_df.iterrows():
            start_time = pd.to_datetime(row['created_at'])
            end_time = start_time + timedelta(minutes=1)

            # Calculate the mean values for the entire 2-minute period
            mean_clicks = row['clicks'] / 2
            mean_impressions = row['impressions'] / 2
            mean_revenue = row['revenue'] / 2
            mean_conversions = row['conversions'] / 2

            # Split the mean values into two 1-minute intervals
            google_split_data.append({
                'id': row['id'],  # Retain the same id
                'created_at': start_time,
                'clicks': mean_clicks,  # Mean value for the first 1-minute interval
                'impressions': mean_impressions,
                'revenue': mean_revenue,
                'conversions': mean_conversions
            })
            google_split_data.append({
                'id': row['id'],  # Retain the same id
                'created_at': end_time,
                'clicks': mean_clicks,  # Mean value for the second 1-minute interval
                'impressions': mean_impressions,
                'revenue': mean_revenue,
                'conversions': mean_conversions
            })

        # Convert the list of dictionaries into a DataFrame
        google_split_df = pd.DataFrame(google_split_data)

        # Merge the Google data with the company data based on 'id' and 'created_at'
        merged_df = pd.merge(company_df, google_split_df, on=['id', 'created_at'], suffixes=('_company', '_google'))


        # Add raw values for understanding
        merged_df['company_clicks'] = merged_df['clicks_company']
        merged_df['company_impressions'] = merged_df['impressions_company']
        merged_df['google_impressions'] = merged_df['impressions_google']
        merged_df['google_clicks'] = merged_df['clicks_google']
        merged_df['spending'] = merged_df['spending']
        merged_df['revenue'] = merged_df['revenue']
        
        merged_df.drop(columns=[
            'clicks_company', 'impressions_company',  
            'clicks_google', 'impressions_google', 'Ad_Type', 
            'Campaign_Goal', 'Geographic_Targeting', 'Platform_Type', 'Target_Audience_Demographics'   
        ], inplace=True)

        # Calculate metrics for each 1-minute interval
        merged_df['clicks_discrepancy'] = merged_df['google_clicks'] - merged_df['company_clicks']
        merged_df['impressions_discrepancy'] = merged_df['google_impressions'] - merged_df['company_impressions']
        merged_df['profit'] = merged_df['revenue'] - merged_df['spending']
        merged_df['roi'] = (merged_df['profit'] / merged_df['spending']) * 100
        merged_df['ctr_google'] = (merged_df['google_clicks'] / merged_df['google_impressions']) * 100
        merged_df['ctr_company'] = (merged_df['company_clicks'] / merged_df['company_impressions']) * 100
        merged_df['conversion_rate_google'] = (merged_df['conversions_google'] / merged_df['google_clicks']) * 100
        merged_df['conversion_rate_company'] = (merged_df['conversions_company'] / merged_df['company_clicks']) * 100
        
        
        merged_df['clicks_discrepancy'] = merged_df['clicks_discrepancy'].abs()
        merged_df['impressions_discrepancy'] = merged_df['impressions_discrepancy'].abs()
        merged_df['profit'] = merged_df['profit'].abs()

        # Save the statistics to a CSV for the current timeframe
        stats_file = 'statistics_with_split_data.csv'
        merged_df.to_csv(stats_file, index=False)
        print(f"Statistics calculated and saved to {stats_file}.")

    except Exception as e:
        print(f"Error calculating statistics: {e}")