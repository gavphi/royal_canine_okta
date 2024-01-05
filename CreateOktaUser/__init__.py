import logging
from api_functions.okta import create_user, get_user, update_user
from api_functions.utils import load_json
from azure_storage import AzureStorage
import azure.functions as func
from core import config
import pandas as pd
import json
from datetime import datetime
from api_functions.db_functions import parse_from_sql, update_okta_table
from api_functions.db_schemas import UsersOKTA_TblSchema

account_type = 'test'

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Creating new users in OKTA')
    
    azs = AzureStorage(config.azure_config.container_name)

    #input_config = req.get_json()
    input_config = {"page": "lifestage_dog"}
    page = input_config['page']

    start_date = "2023-06-25"
    end_date = "2023-06-29"

    
    #user_data = azs.download_blob_df(f"{page}/sfmc_data_{start_date}_{end_date}.csv")   

    query = f"""SELECT * from UsersSFMC us where registry_date > '{start_date} 00:00:00.000' and registry_date < '{end_date} 00:00:00.000'"""
    user_data = parse_from_sql(query)

    logging.info(f"user_data: {user_data}")
    
    ids = []
    
    #for index, row in user_data.iterrows():
    users_df = user_data.iloc[14]

    #try:
    logging.info("Inserting user")

    res = create_user(users_df.to_dict())

    id = json.loads(res.text)['id']

    users_df["id"] = id
    
    users_df["registry_date"] = pd.to_datetime(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))

    users_df.to_csv(f"users_df.csv")
    
    logging.info(f"Status code: {res.status_code}")
    if res.status_code == 200:
        #df.to_csv("dataframe_id.csv")

        update_okta_table(users_df, "UsersOkta", UsersOKTA_TblSchema())
        azs.upload_blob_df(pd.DataFrame(data=users_df), f"{page}/okta/okta_data_{start_date}_{end_date}.csv")
        return func.HttpResponse("New user created in Okta")

    elif res.status_code == 409:
        logging.warning(f"User already exists. Updating fields.")
        
        res = update_user(users_df.to_dict())

        logging.warning(f"Response OKA: {res.text}")
        return func.HttpResponse("Got info from OKTA")
        
    azs.upload_blob_df(pd.DataFrame(data=users_df), f"{page}/failed_cases/sfmc_data_{start_date}_{end_date}.csv")

    
