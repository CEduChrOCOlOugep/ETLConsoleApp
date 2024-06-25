import asyncio
import base64
import logging
import os
import time
import traceback
from pathlib import Path

import aiohttp
import numpy as np
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(Path("../logs/App.log")),
                        logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Define the path to the data directory
data_directory = Path('../data')

# take environment variables from .env.
load_dotenv('../.env')
br03 = os.getenv("APP_ID3")
br04 = os.getenv("APP_ID4")
api_base_url = os.getenv("API_BASE_URL")
username = os.getenv("API_USERNAME")
password = os.getenv("API_PASSWORD")
encoded_credentials = base64.b64encode(f'{username}:{password}'.encode('utf-8')).decode('utf-8')


def create_payload(appid, page_number=1, page_size="5000", fieldname="LAST_UPDATE",
                   last_update="20240501000000") -> dict:
    return {
        "appid": appid,
        "pagenumber": page_number,
        "pagesize": page_size,
        "filter": {
            "query": [
                {
                    "fieldname": fieldname,
                    "operand": ">=",
                    "fieldvalue": last_update
                }
            ]
        }
    }


# Async function to fetch data with pagination and save to CSV
async def fetch_data(session: aiohttp.ClientSession, appid: str) -> pd.DataFrame:
    all_data = []
    page = 1
    page_size = 5000
    start_time = time.time()
    logger.info(f"Start time for appid {appid}: {start_time}")

    while True:
        try:
            data = create_payload(appid, page, page_size)
            logger.info(f"Fetching data for appid {appid}, page {page}")

            headers = {
                'Authorization': f'Basic {encoded_credentials}',
                'Content-Type': 'application/json'
            }
            async with session.post(api_base_url, json=data, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch data: {response.status}")
                response_data = await response.json()
                logger.info(f"Fetched page {page} for appid {appid}")

                for row in response_data.get('row', []):
                    row_data = {field['name']: field['value'] for field in row['field']}
                    all_data.append(row_data)

                total_records = int(response_data.get('totalrecordcount', '0'))
                if page * page_size >= total_records:
                    break

                page += 1
        except Exception as e:
            logger.error(f"Error occurred while fetching data for appid {appid}, page {page}: {e}")
            logger.error(traceback.format_exc())
            return None

    df = pd.DataFrame(all_data)

    if df.empty:
        logger.error(f"No data fetched for appid {appid}.")
        return None

    logger.info(f"Fetched data for appid {appid}, total records: {len(df)}")
    df = df.dropna(subset=['EEEIN'])

    # Convert all columns to string
    df = ensure_string_columns(df)

    # Debugging breakpoint: Inspect the types of all columns after fetching data
    print("After fetching data:", df.dtypes)  # Temporary print statement for debugging

    if appid == br03:
        filename = 'br03.csv'
    elif appid == br04:
        filename = 'br04.csv'
    df.to_csv(os.path.join(data_directory / filename), index=False)

    end_time = time.time()
    processing_time = end_time - start_time
    logger.info(
        f"All data for appid {appid} has been fetched and saved to CSV. Processing time: {processing_time:.2f} seconds.")
    return df


def read_and_convert_to_string(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path, dtype=str)
        df = ensure_string_columns(df)

        # Debugging breakpoint: Inspect the types of all columns after reading CSV
        print("After reading CSV:", df.dtypes)  # Temporary print statement for debugging

        return df
    except Exception as e:
        logger.error(f"Error occurred while reading CSV file {file_path}: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def ensure_string_columns(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
    try:
        if columns is None:
            columns = df.columns
        for column in columns:
            if column in df.columns:
                df[column] = df[column].astype(str)
        return df
    except Exception as e:
        logger.error(f"Error occurred while ensuring string columns: {e}")
        logger.error(traceback.format_exc())
        return df


def replace_missing_with_default(df: pd.DataFrame) -> pd.DataFrame:
    try:
        for index, row in df.iterrows():
            if row['ESCD2'] == 'A':
                df.at[index, 'SBGDT'] = ''
            elif row['ESCD2'] == 'S' and pd.isna(row['SBGDT']):
                df.at[index, 'SBGDT'] = ''
        return df
    except Exception as e:
        logger.error(f"Error occurred while replacing missing values: {e}")
        logger.error(traceback.format_exc())
        return df


def save_dataframe_as_csv(df: pd.DataFrame, output_file_path: str):
    try:
        df.to_csv(output_file_path, index=False)
    except Exception as e:
        logger.error(f"Error occurred while saving DataFrame to CSV: {e}")
        logger.error(traceback.format_exc())


# Main async function to handle the request
async def main():
    try:
        async with aiohttp.ClientSession() as session:
            # Combine them into a list
            appids = [br03, br04]
            # Create tasks for each appid
            tasks = [fetch_data(session, appid) for appid in appids]
            # Run tasks concurrently
            fetched_dfs = await asyncio.gather(*tasks)

            # Proceed if both DataFrames are successfully fetched
            if all(df is not None for df in fetched_dfs):
                # Read and convert data to string
                df1 = read_and_convert_to_string(data_directory / 'br03.csv')
                df2 = read_and_convert_to_string(data_directory / 'br04.csv')

                # Merge the dataframes on the 'EEEIN' column
                final_df = pd.merge(df1, df2, on='EEEIN', how='outer')

                # Debugging breakpoint: Inspect the types of all columns after merging dataframes
                print("After merging dataframes:", final_df.dtypes)  # Temporary print statement for debugging

                # Ensure specific columns are strings
                columns_to_ensure = ['EEEIN', 'SSSN', 'ACTION', 'A', 'E2', 'S', 'D', 'F', 'G', 'H']
                final_df = ensure_string_columns(final_df, columns_to_ensure)

                # Replace missing values with the default values
                final_df = replace_missing_with_default(final_df)

                # Modify the 'ActionType' column based on the 'E2' column
                final_df['hActionType'] = final_df['E2'].apply(
                    lambda x: 'hRescindActionRequested' if x == 'S' else ('NoActionRequested' if x == 'A' else 'NotSet'))

                # Debugging breakpoint: Inspect the types of all columns before saving to CSV
                print("Before saving to CSV:", final_df.dtypes)  # Temporary print statement for debugging

                # Save the final DataFrame as a CSV file
                output_file_path = data_directory / 'h_scd.csv'
                save_dataframe_as_csv(final_df, output_file_path)

                # Log the shape of the DataFrame
                logger.info(f"DataFrame has {final_df.shape[0]} rows and {final_df.shape[1]} columns")
    except Exception as e:
        logger.error(f"Error occurred in main function: {e}")
        logger.error(traceback.format_exc())


# Run the main function
try:
    asyncio.run(main())
except Exception as e:
    logger.error(f"Error occurred while running the main function: {e}")
    logger.error(traceback.format_exc())