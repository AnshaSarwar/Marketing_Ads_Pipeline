import logging
from apscheduler.schedulers.background import BackgroundScheduler
from data_generation import generate_company_data, generate_google_data
from data_storage import store_data, fetch_data
from data_processing import calculate_statistics
from db_connection import get_db_connection


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# Function to process and save company data
def load_company_data(company_collection):
    company_data = generate_company_data()
    store_data(company_collection, company_data)
    company_df = fetch_data(company_collection)
    company_df.to_csv('company_data.csv', index=False)
    logger.info("Company data saved to company_data.csv.")

# Function to process and save Google data
def load_google_data(google_collection):
    google_data = generate_google_data()
    store_data(google_collection, google_data)
    google_df = fetch_data(google_collection)
    google_df.to_csv('google_data.csv', index=False)
    logger.info("Google data saved to company_data.csv.")

# Main function to initialize scheduler
def main():
    
   # MongoDB connection setup
    db = get_db_connection()
    
    # Ask the user if they want to delete the database
    user_input = input("Do you want to delete the database? (yes/no): ").strip().lower()
    if user_input == "yes":
        logger.warning("Deleting the database...")
        db.client.drop_database(db.name)
        logger.info(f"Database '{db.name}' has been deleted.")
        return  # Exit after deletion

    # Immediate execution
    logger.info("Immediate execution of data loading.")
    
    company_collection = db['company_dataset']
    google_collection = db['google_dataset']
    
    # Immediate execution
    logger.info("Immediate execution of data loading.")
    load_company_data(company_collection)
    load_google_data(google_collection)

    # Scheduler setup
    scheduler = BackgroundScheduler()
    scheduler.add_job(load_company_data, 'interval', minutes=1, id='company_data_job', args=[company_collection])
    scheduler.add_job(load_google_data, 'interval', minutes=2, id='google_data_job', args=[google_collection])
    scheduler.add_job(calculate_statistics, 'interval', minutes=2, id='statistics_job', args=[company_collection, google_collection])

    scheduler.start()

    logger.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        while True:
            pass  # Keep the program running
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    main()