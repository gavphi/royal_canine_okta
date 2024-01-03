import logging
from api_functions.salesforce_connection import get_token, get_data, transform_data, post_process_name_surname
from api_functions.utils import load_json
import azure.functions as func
import pandas as pd
from core import config
from azure_storage import AzureStorage

page = "start_of_life_dogs"
config_json = load_json("Extract_Start_Of_Life_Dog/config.json")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Extract user data from Salesforce')
    
    transform = None

    sfmc_token = get_token()

    clave = config_json[page]["clave"]

    date_column = config_json[page]["date_column"]

    start_date = "2023-12-12"
    end_date = "2023-12-13"
    
    try:
        data = get_data(sfmc_token, start_date, end_date, clave, date_column)
        transform = True
    except:
        logging.warning(f"No Data to be extract for {page} today or something went wrong.")

    if transform:
        users = post_process_name_surname(transform_data(data, page))

        azs = AzureStorage(config.azure_config.container_name)

        azs.upload_blob_df(pd.DataFrame(data=users), f"{page}/sfmc_data_{start_date}_{end_date}.csv")

        return func.HttpResponse("Data Extracted from SFMC")
    else:
        return func.HttpResponse(f"No Data to be extract for page {page} from SFMC")