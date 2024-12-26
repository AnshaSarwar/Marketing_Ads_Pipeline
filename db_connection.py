from pymongo import MongoClient
import logging

# Set up logging
logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Establishes a connection to MongoDB and returns the database connection.
    """
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client['Ads']
        logger.info("MongoDB connection established.")
        return db
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise
