import logging
from api_functions.salesforce_connection import *
from api_functions.utils import load_json
import azure.functions as func
import pandas as pd
from core import config
from azure_storage import AzureStorage, save_logs
from datetime import datetime, timedelta
from api_functions.db_functions import *
from api_functions.db_schemas import UsersSFMC_TblSchema, OneTrustConsents_TblSchema
import io

config_json = load_json("config.json")

split_name_pages = ["start_of_life_kittens", "start_of_life_dogs", "lifestage_dog", "start_of_life_dog_breeders", "start_of_life_kittens_breeders"]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Extract user data from Salesforce')
    

    page = req.get_body().decode('utf-8')
    #page = "calculadora"

    logging.info(f"Extracting for page... {page}")

    LOG_PATH = f"{page}/logs/logs.log"

    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger()
    logger.addHandler(handler)


    sfmc_token = get_token()

    clave = config_json[page]["clave"]

    date_column = config_json[page]["date_column"]

    withdrawl = config_json[page]["withdrawl"]
    
    start_date = "2020-01-01" #datetime.today().strftime('%Y-%m-%d')

    today = datetime.today()
    day_after = today + timedelta(days=1)
    end_date = day_after.strftime('%Y-%m-%d')

    current_time = "2024-01-09" #datetime.today().strftime('%Y-%m-%d %H:%M')
    
    '''
    Get raw data from SFMC using SFMC API endpoint.
    '''
    try:
        logging.info(f"Getting data for page... {page}")
        data = get_data(sfmc_token, start_date, end_date, clave, date_column)
    except:
        logging.warning(f"No Data to be extract for {page} today or something went wrong.")
        save_logs(log_stream.getvalue(),  current_time, LOG_PATH)
        return func.HttpResponse(f"No Data to be extract for {page} today or something went wrong", status_code = 200)

    '''
    Process data of all LP except withdrawl LP to DataFrame and save in Azure Storage
    '''

    logging.info(f"Data: {data}")
    if data != []:
        logging.info(f"Transforming data for page... {page}")
        users = transform_data(data, page)

        if page in split_name_pages:
            users = post_process_name_surname(users)

        users_df = prepare_df(users, page)

        users_df.to_csv("users.csv")
        logging.info(f"Uploading to DB... {page}")
        #update_sfmc_table(users_df[["name","surname","email","mobilephone","lng","countryCode", "registry_date", "data_extension", "last_update"]], "UsersSFMC", UsersSFMC_TblSchema())
        
        update_consents_table(users_df[["email", "mars_petcare_consent","rc_mkt_consent","data_research_consent","rc_tyc_consent", "last_update", "withdrawl"]], "OneTrustConsents", OneTrustConsents_TblSchema())

        azs = AzureStorage(config.azure_config.container_name)
        azs.upload_blob_df(pd.DataFrame(data=users), f"{page}/sfmc_data_{start_date}_{end_date}.csv")

        return func.HttpResponse("Data Extracted from SFMC")

    '''
    Process data of withdrawl LP to DataFrame and save in Azure Storage
    '''
    if withdrawl and data != []:
        users = transform_withdrawl_data(data)

        azs = AzureStorage(config.azure_config.container_name)

        azs.upload_blob_df(pd.DataFrame(data=users), f"{page}/withdrawl/sfmc_data_{start_date}_{end_date}.csv")

        save_logs(log_stream.getvalue(), current_time, LOG_PATH)
        return func.HttpResponse("Users to Withdrawl extracted from SFMC")
        
    else:

        save_logs(log_stream.getvalue(), current_time, LOG_PATH)
        return func.HttpResponse(f"No Data to be extract for page {page} from SFMC", status_code=500)