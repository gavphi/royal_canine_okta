import logging
from api_functions.salesforce_connection import get_token, get_data, transform_data
from api_functions.grant_consent import granting_consent
from api_functions.utils import load_json
import json
from azure_storage import AzureStorage
from core import config
import azure.functions as func
import io


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Creating consent to new users in OKTA')

    azs = AzureStorage(config.azure_config.container_name)

    page = 'calculadora'
    start_date = "2023-12-12"
    end_date = "2023-12-13"

    user_data = azs.download_blob_df(f"{page}/sfmc_data_{start_date}_{end_date}.csv")   
    
    res = granting_consent(id_test)

    print(res.status_code)
    print(res.text)

    return func.HttpResponse("New user created in Okta")
