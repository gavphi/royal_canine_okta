import logging
from api_functions.salesforce_connection import get_token, get_data, transform_data
from api_functions.grant_consent import withdrawl_consent
from api_functions.utils import load_json
from azure_storage import AzureStorage
import azure.functions as func
from core import config
import pandas as pd
import json
from api_functions.db_functions import parse_from_sql

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Creating new users in OKTA')
    
    azs = AzureStorage(config.azure_config.container_name)

    #input_config = req.get_json()
    input_config = {"page": "unsubscribe"}
    page = input_config['page']

    start_date = "2023-06-25"
    end_date = "2023-06-29"

    user_data = azs.download_blob_df(f"{page}/withdrawl/sfmc_data_{start_date}_{end_date}.csv")   

    query = f"""SELECT us.id
            FROM UsersSFMC us
            JOIN OneTrustConsents otc ON otc.email = us.email
            WHERE us.registry_date > '{start_date} 00:00:00.000' AND us.registry_date < '{end_date} 00:00:00.000'
            and otc.withdrawl = 1
            """
    user_data = parse_from_sql(query)

    res = withdrawl_consent(user_data["id"], config.consents_config.mars_petcare_consent )
    res = withdrawl_consent(user_data["id"], config.consents_config.data_research_consent )
    res = withdrawl_consent(user_data["id"], config.consents_config.rc_mkt_consent )
    res = withdrawl_consent(user_data["id"], config.consents_config.rc_tyc_consent )


    
