import pandas as pd
from pymongo import MongoClient, UpdateOne
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Function to store data in MongoDB and update records
def store_data(collection, data):
    bulk_operations = []
    for record in data:
        bulk_operations.append(UpdateOne(
            {'id': record['id'], 'created_at': record['created_at']},
            {'$set': record}, upsert=True
        ))
    try:
        collection.bulk_write(bulk_operations)
        logger.info(f"{len(bulk_operations)} records processed for {collection.name}.")
    except Exception as e:
        logger.error(f"Error during bulk write for {collection.name}: {e}")

# Function to fetch data from a MongoDB collection
def fetch_data(collection):
    try:
        return pd.DataFrame(list(collection.find()))
    except Exception as e:
        logger.error(f"Error fetching data from {collection.name}: {e}")
        return pd.DataFrame()
