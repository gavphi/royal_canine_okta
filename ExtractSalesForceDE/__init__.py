import logging
from api_functions.salesforce_connection import *
from api_functions.utils import load_json
import azure.functions as func
import pandas as pd
from core import config
from azure_storage import AzureStorage
from datetime import datetime
from api_functions.db_functions import *
from api_functions.db_schemas import UsersSFMC_TblSchema, OneTrustConsents_TblSchema

config_json = load_json("config.json")

split_name_pages = ["start_of_life_kittens", "start_of_life_dogs", "lifestage_dog"]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Extract user data from Salesforce')
    
    transform = None

    #input_config = req.get_json()

    input_config = {"page": "lifestage_dog"}

    page = input_config["page"]

    sfmc_token = get_token()

    clave = config_json[page]["clave"]

    date_column = config_json[page]["date_column"]

    withdrawl = config_json[page]["withdrawl"]
    
    start_date = "2023-06-25"
    end_date = "2023-06-29"
    
    '''
    Get raw data from SFMC using SFMC API endpoint.
    '''
    #try:
    data = get_data(sfmc_token, start_date, end_date, clave, date_column)
    
    if withdrawl == False:
        transform = True
    #except:
    #    logging.warning(f"No Data to be extract for {page} today or something went wrong.")

    '''
    Process data of all LP except withdrawl LP to DataFrame and save in Azure Storage
    '''
    if transform:
        users = transform_data(data, page)

        if page in split_name_pages:
            users = post_process_name_surname(users)

        users_df = prepare_df(users)
        
        update_sfmc_table(users_df[["name","surname","email","mobilephone","lng","countryCode", "registry_date"]], "UsersSFMC", UsersSFMC_TblSchema())
        
        update_consents_table(users_df[["email", "mars_petcare_consent","rc_mkt_consent","data_research_consent","rc_tyc_consent", "last_update"]], "OneTrustConsents", OneTrustConsents_TblSchema())

        azs = AzureStorage(config.azure_config.container_name)
        azs.upload_blob_df(pd.DataFrame(data=users), f"{page}/sfmc_data_{start_date}_{end_date}.csv")

        return func.HttpResponse("Data Extracted from SFMC")

    '''
    Process data of withdrawl LP to DataFrame and save in Azure Storage
    '''
    if withdrawl:
        users = transform_withdrawl_data(data)

        azs = AzureStorage(config.azure_config.container_name)

        azs.upload_blob_df(pd.DataFrame(data=users), f"{page}/withdrawl/sfmc_data_{start_date}_{end_date}.csv")

        return func.HttpResponse("Users to Withdrawl extracted from SFMC")
        
    else:
        return func.HttpResponse(f"No Data to be extract for page {page} from SFMC")