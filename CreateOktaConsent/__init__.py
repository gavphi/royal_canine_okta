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

    #input_config = req.get_json()
    input_config = {"page": "lifestage_dog"}
    page = input_config['page']

    start_date = "2023-06-25"
    end_date = "2023-06-29"

    user_data = azs.download_blob_df(f"{page}/okta/okta_data_{start_date}_{end_date}.csv")   
    
    res = granting_consent(user_data["okta_id"])

    logging.info(f"RESPONSE STATUS CODE: {res.status_code}")
    logging.info(f"RESPONSE TEXT: {res.text}")

    return func.HttpResponse("New user created in Okta")
