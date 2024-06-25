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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(Path("../logs/App.log")), logging.StreamHandler()])
logger = logging.getLogger(__name__)
data_directory = Path('../data')
load_dotenv('../.env')
br03 = os.getenv("APP_ID3")
br04 = os.getenv("APP_ID4")
api_base_url = os.getenv("API_BASE_URL")
username = os.getenv("API_USERNAME")
password = os.getenv("API_PASSWORD")
encoded_credentials = base64.b64encode(f'{username}:{password}'.encode('utf-8')).decode('utf-8')

def create_payload(appid, page_number=1, page_size="5000", fieldname="LAST_UPDATE", last_update="20240501000000") -> dict:
    return {"appid": appid, "pagenumber": page_number, "pagesize": page_size, "filter": {"query": [{"fieldname": fieldname, "operand": ">=", "fieldvalue": last_update}]}}

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
            headers = {'Authorization': f'Basic {encoded_credentials}', 'Content-Type': 'application/json'}
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
    df = ensure_string_columns(df)
    print("After fetching data:", df.dtypes)
    if appid == br03:
        filename = 'br03.csv'
    elif appid == br04:
        filename = 'br04.csv'
    df.to_csv(os.path.join(data_directory / filename), index=False)
    end_time = time.time()
    processing_time = end_time - start_time
    logger.info(f"All data for appid {appid} has been fetched and saved to CSV. Processing time: {processing_time:.2f} seconds.")
    return df

def read_and_convert_to_string(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path, dtype=str)
        df = ensure_string_columns(df)
        print("After reading CSV:", df.dtypes)
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
            elif row['ESCD2'] == 'S' and (pd.isna(row['SBGDT']) or row['SBGDT'] == 'nan'):
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

def load_and_check_csv(file_path: str, check_col, check_val, range_col, id_col, output_file) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path, dtype=str)
        condition = (df[check_col] == check_val) & ~df[range_col].between('300', '399')
        correction_condition = (df[range_col] == '800')
        error_rows = df[condition & ~correction_condition]
        correction_rows = df[correction_condition].copy()
        if not correction_rows.empty:
            correction_rows['CorrectionFlag'] = 'True'
            correction_output_file = os.path.splitext(file_path)[0] + '_with_correction_flag.csv'
            correction_rows.to_csv(correction_output_file, index=False)
            logger.info(f"Records with CorrectionFlag saved to {correction_output_file}")
        if not error_rows.empty:
            error_rows[[id_col]].to_csv(output_file, index=False)
            logger.info(f"Errors logged in {output_file}")
            df_without_errors = df[~condition].copy()
            df_without_errors = df_without_errors.astype(str)
            error_free_output_file = os.path.splitext(file_path)[0] + '_without_errors.csv'
            df_without_errors.to_csv(error_free_output_file, index=False)
            logger.info(f"Error-free data saved to {error_free_output_file}")
        else:
            logger.info("No errors found.")
            df_without_errors = df.copy()
        return df_without_errors
    except Exception as e:
        logger.error(f"Failed to load and check CSV file {file_path}: {e}")
        logger.error(traceback.format_exc())
        raise

async def main():
    try:
        async with aiohttp.ClientSession() as session:
            appids = [br03, br04]
            tasks = [fetch_data(session, appid) for appid in appids]
            fetched_dfs = await asyncio.gather(*tasks)
            if all(df is not None for df in fetched_dfs):
                df1 = read_and_convert_to_string(data_directory / 'br03.csv')
                df2 = read_and_convert_to_string(data_directory / 'br04.csv')
                df2 = replace_missing_with_default(df2)
                final_df = pd.merge(df1, df2, on='EEEIN', how='outer')
                print("After merging dataframes:", final_df.dtypes)
                columns_to_ensure = ['EEEIN', 'SSSN', 'ACTION', 'A', 'E2', 'S', 'D', 'F', 'G', 'H']
                final_df = ensure_string_columns(final_df, columns_to_ensure)
                final_df = replace_missing_with_default(final_df)
                final_df['hActionType'] = final_df['E2'].apply(lambda x: 'hRescindActionRequested' if x == 'S' else ('NoActionRequested' if x == 'A' else 'NotSet'))
                print("Before saving to CSV:", final_df.dtypes)
                output_file_path = data_directory / 'h_scd.csv'
                save_dataframe_as_csv(final_df, output_file_path)
                check_col = 'E2'
                check_val = 'S'
                range_col = 'ACTION'
                id_col = 'SSSN'
                error_output_file = data_directory / 'error_log.csv'
                final_df = load_and_check_csv(output_file_path, check_col, check_val, range_col, id_col, error_output_file)
                logger.info(f"DataFrame has {final_df.shape[0]} rows and {final_df.shape[1]} columns")
    except Exception as e:
        logger.error(f"Error occurred in main function: {e}")
        logger.error(traceback.format_exc())

try:
    asyncio.run(main())
except Exception as e:
    logger.error(f"Error occurred while running the main function: {e}")
    logger.error(traceback.format_exc())