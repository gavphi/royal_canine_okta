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
    
    #try:
    #page = req.get_body().decode('utf-8')
    
    
    page = "unsubcribed"
    logging.info(f'Extract user data from Salesforce for {page}')

    logging.info(f"Extracting for page... {page}")

    current_time = datetime.today().strftime('%Y-%m-%d %H:%M')

    LOG_PATH = f"{page}/logs/logs_{current_time}.log"

    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger()
    logger.addHandler(handler)


    sfmc_token = get_token()

    clave = config_json[page]["clave"]

    date_column = config_json[page]["date_column"]

    withdrawl = config_json[page]["withdrawl"]
    
    start_date = "2023-06-27" #datetime.today().strftime('%Y-%m-%d')

    today = datetime.today()
    day_after = today + timedelta(days=1)
    end_date = "2023-06-28" #day_after.strftime('%Y-%m-%d')
    
    '''
    Get raw data from SFMC using SFMC API endpoint.
    '''
    try:
        logging.info(f"Getting data for page... {page}")
        data = get_data(sfmc_token, start_date, end_date, clave, date_column)
    except:
        logging.warning(f"No Data to be extract for {page} today or something went wrong.")
        save_logs(log_stream.getvalue(), LOG_PATH)
        return func.HttpResponse(f"No Data to be extract for {page} today or something went wrong")

    '''
    Process data of all LP except withdrawl LP to DataFrame and save in Azure Storage
    '''


    if data != [] and withdrawl == 0:
        logging.info(f"Transforming data for page... {page}")
        users = transform_data(data, page, withdrawl)

        if page in split_name_pages:
            users = post_process_name_surname(users)

        users_df = prepare_df(users, page)

        logging.info(f"Uploading to DB... {page}")
        update_sfmc_table(users_df[["name","surname","email","mobilephone","lng","countryCode", "registry_date", "data_extension", "last_update"]], "UsersSFMC", UsersSFMC_TblSchema())
        
        update_consents_table(users_df[["email", "mars_petcare_consent","rc_mkt_consent","data_research_consent","rc_tyc_consent", "last_update", "withdrawl", "registry_date"]], "OneTrustConsent", OneTrustConsents_TblSchema())

        azs = AzureStorage(config.azure_config.container_name)
        azs.upload_blob_df(pd.DataFrame(data=users), f"{page}/sfmc_data_{start_date}_{end_date}.csv")

        return func.HttpResponse("Data Extracted from SFMC")

    '''
    Process data of withdrawl LP to DataFrame and save in Azure Storage
    '''
    if withdrawl and data != []:
        users = transform_withdrawl_data(data)

        update_consents_table(users[["email", "last_update", "withdrawl", "registry_date"]], "OneTrustConsent", OneTrustConsents_TblSchema(), True)

        save_logs(log_stream.getvalue(), LOG_PATH)
        return func.HttpResponse("Users to Withdrawl extracted from SFMC")
    else:
        save_logs(log_stream.getvalue(), LOG_PATH)
        azs = AzureStorage(config.azure_config.container_name)
        azs.upload_blob_df(pd.DataFrame(data=users), f"{page}/withdrawl_fails/sfmc_data_{start_date}_{end_date}.csv")
        return func.HttpResponse(f"No Data to be extract for page {page} from SFMC", status_code=200)
    """except Exception as e:

        return func.HttpResponse(f"Error {e}", status_code=400)"""