# **Ads Data Pipeline**

This project implements a data pipeline that simulates and processes company and Google advertising data in real time (1 min for company data, 2 min for google data). The pipeline collects, stores, and processes data in MongoDB, calculates key performance metrics, and saves the results to CSV files. 
The pipeline includes scheduling for automated data generation and statistics calculation.

## **Overview**

The Ads Data Pipeline simulates two types of advertising data:

-**Company Data:** Simulated data related to advertising campaigns from various companies.

-**Google Data:** Simulated advertising data sourced from Google.

Both datasets are stored in MongoDB, and the pipeline continuously processes the data, calculates various performance metrics (such as conversion rates, clicks discrepancies, and ROI), and stores the results in CSV files.

The pipeline is built with scheduling to generate and update the data at regular intervals (new company data would be inserted after 1 min and new google data would be inserted after 2 min), along with calculation of statistics.

## **Features :**

-**Simulated Data Generation:** Randomized company and Google advertising data.

**-Data Storage:** MongoDB is used to store the data.

-**Data Fetching:** The pipeline fetches data from the MongoDB collections into Pandas DataFrames for processing.

-**Real-Time Data Processing:** Calculates discrepancies, conversion rates, ROI, and other KPIs.

-**Statistics Calculation:** The pipeline computes performance metrics every minute and saves them to CSV files.

-**Scheduled Tasks:** Jobs to generate new data and calculate statistics at regular intervals.

## **Requirements**

-Python 3.x

-MongoDB running on localhost:27017

-The following Python packages:  numpy, pandas, pymongo, apscheduler,logging
