import logging
from api_functions.salesforce_connection import *
from api_functions.utils import load_json, setup_logger
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
    
    # Get input variables
    input_config = req.get_body().decode('utf-8')
    page = input_config["page"]
    historical_migration = input_config["historical_migration"]
    start_date = input_config["start_date"]
    end_date = input_config["end_date"]

    # Get Data Extension configurations
    clave = config_json[page]["clave"]
    date_column = config_json[page]["date_column"]
    withdrawl = config_json[page]["withdrawl"]

    # Get interval of dates 
    current_time = datetime.today().strftime('%Y-%m-%d %H:%M')
    today = datetime.today()
    day_after = today + timedelta(days=1)

    if not start_date:
        start_date = datetime.today().strftime('%Y-%m-%d')

    if not end_date:
        end_date = day_after.strftime('%Y-%m-%d')
        
    # Setup logging
    LOG_PATH = f"{page}/logs/logs_{current_time}.log"
    logger, log_stream = setup_logger()

    logging.info(f'Extract user data from Salesforce for {page}')

    # Get token
    sfmc_token = get_token()

    
    #Get raw data from SFMC using SFMC API endpoint
    try:
        
        if historical_migration:
            data = get_all_data(sfmc_token, clave)
        else:
            logging.info(f"Getting data for {page} between dates.")
            data = get_data(sfmc_token, start_date, end_date, clave, date_column)
        
        save_logs(log_stream.getvalue(), LOG_PATH)
    
    except:
        logging.warning(f"No Data to be extract for {page} today or something went wrong.")
        save_logs(log_stream.getvalue(), LOG_PATH)
        return func.HttpResponse(f"No Data to be extract for {page} today or something went wrong")

    
    # Process data of all LP except withdrawl LP to DataFrame and save in Azure Storage
    
    if data != [] and withdrawl == 0:
        logging.info(f"Transforming data for page... {page}")
        users = transform_data(data, page, withdrawl)

        if page in split_name_pages:
            users = post_process_name_surname(users)

        users_df = prepare_df(users, page)

        users_df.to_csv(f"{page}.csv")
        logging.info(f"Uploading to DB... {page}")
        update_sfmc_table(users_df[["name","surname","email","mobilephone","lng","countryCode", "registry_date", "data_extension", "last_update"]], "UsersSFMC", UsersSFMC_TblSchema())
        
        update_consents_table(users_df[["email", "mars_petcare_consent","rc_mkt_consent","data_research_consent","rc_tyc_consent", "last_update", "registry_date"]], "OneTrustConsent", OneTrustConsents_TblSchema())


        return func.HttpResponse("Data Extracted from SFMC")

    
    # Process data of withdrawl LP to DataFrame and save in Azure Storage
    if withdrawl and data != []:
        users = transform_withdrawl_data(data)

        update_consents_table(users[["email", "last_update", "withdrawl", "registry_date"]], "OneTrustConsent", OneTrustConsents_TblSchema(), True)

        save_logs(log_stream.getvalue(), LOG_PATH)
        return func.HttpResponse("Users to Withdrawl extracted from SFMC")
    else:
        save_logs(log_stream.getvalue(), LOG_PATH)
        return func.HttpResponse(f"No Data to be extract for page {page} from SFMC", status_code=200)
    