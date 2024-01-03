import logging
from api_functions.salesforce_connection import get_token, get_data, transform_data
from api_functions.okta import create_user
from api_functions.utils import load_json
from azure_storage import AzureStorage
import azure.functions as func
from core import config
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Creating new users in OKTA')
    
    azs = AzureStorage(config.azure_config.container_name)

    page = 'calculadora'
    start_date = "2023-12-12"
    end_date = "2023-12-13"

    user_data = azs.download_blob_df(f"{page}/sfmc_data_{start_date}_{end_date}.csv")   

    i = 0
    ids = []
    df = pd.DataFrame()
    for index, row in user_data.iterrows():

        if i < 3:
            try:
                logging.info("Inserting user")
                logging.info(f"row: {row.to_dict()}")
                res = create_user(row.to_dict())
                logging.info(f"Response: {res.status_code}")
                """
                id = res.to_json().id
                row["okta_id"] = id
                df = df.append(row, ignore_index=True)"""
                
                
            except:
                logging.warning(f"Couldn't create user. Saving case in storage.")
                
                azs.upload_blob_df(pd.DataFrame(data=row), f"{page}/failed_cases/sfmc_data_{start_date}_{end_date}.csv")
        
        azs.upload_blob_df(pd.DataFrame(data=df), f"{page}/okta/okta_data_{start_date}_{end_date}.csv")

    return func.HttpResponse("New user created in Okta")
