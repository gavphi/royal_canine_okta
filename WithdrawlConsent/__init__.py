import logging
from api_functions.salesforce_connection import get_token, get_data, transform_data
from api_functions.okta import create_user, get_user, update_user
from api_functions.utils import load_json
from azure_storage import AzureStorage
import azure.functions as func
from core import config
import pandas as pd
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Creating new users in OKTA')
    
    azs = AzureStorage(config.azure_config.container_name)

    #input_config = req.get_json()
    input_config = {"page": "unsubscribe"}
    page = input_config['page']

    start_date = "2023-06-25"
    end_date = "2023-06-29"

    user_data = azs.download_blob_df(f"{page}/withdrawl/sfmc_data_{start_date}_{end_date}.csv")   

    ids = []
    df = pd.DataFrame(columns=['okta_id'])
    #for index, row in user_data.iterrows():
    row = user_data.iloc[5]
    #try:
    logging.info("Inserting user")

    res = create_user(row.to_dict())
    logging.debug(f"Response: {res.status_code}")
    
    id = json.loads(res.text)['id']

    row["okta_id"] = id
    df.loc[len(df.index)] = row
    
    logging.info(f"Status code: {res.status_code}")
    if res.status_code == 200:
        #df.to_csv("dataframe_id.csv")
        azs.upload_blob_df(pd.DataFrame(data=df), f"{page}/okta/okta_data_{start_date}_{end_date}.csv")
        return func.HttpResponse("New user created in Okta")

    elif res.status_code == 409:
        logging.warning(f"User already exists. Updating fields.")
        
        res = update_user(row.to_dict())

        logging.warning(f"Response OKA: {res.text}")
        return func.HttpResponse("Got info from OKTA")
    """except:
        logging.warning(f"Couldn't create user. Saving case in storage.")"""
        
    azs.upload_blob_df(pd.DataFrame(data=row), f"{page}/failed_cases/sfmc_data_{start_date}_{end_date}.csv")

    
