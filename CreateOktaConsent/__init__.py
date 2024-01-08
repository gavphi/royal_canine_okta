import logging
from api_functions.salesforce_connection import get_token, get_data, transform_data
from api_functions.grant_consent import granting_consent
from api_functions.utils import load_json
import json
from azure_storage import AzureStorage
from core import config
import azure.functions as func
import io

from api_functions.db_functions import parse_from_sql


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Creating consent to new users in OKTA')

    """
    FIX ME: 
    - include loop to create consend per user
    - include retry
    - include try except
    """
    start_date = "2023-06-25"
    end_date = "2023-06-29"


    query = f""" SELECT otc.*, uo.id
            FROM UsersSFMC us
            JOIN OneTrustConsents otc ON otc.email = us.email
            JOIN UsersOkta uo ON uo.email = us.email 
            WHERE us.registry_date > '{start_date} 00:00:00.000' AND us.registry_date < '{end_date} 00:00:00.000'
            and otc.withdrawl = 0
            """
    
    user_data = parse_from_sql(query)

    if user_data["mars_petcare_consent"] == 1:
        res = granting_consent(user_data["id"], config.consents_config.mars_petcare_consent )
    elif user_data["rc_mkt_consent"] == 1:
        res = granting_consent(user_data["id"], config.consents_config.rc_mkt_consent)
    elif user_data["data_research_consent"] == 1:
        res = granting_consent(user_data["id"], config.consents_config.data_research_consent)
    elif user_data["rc_tyc_consent"] == 1:
        res = granting_consent(user_data["id"], config.consents_config.rc_tyc_consent)

    logging.info(f"RESPONSE STATUS CODE: {res.status_code}")
    logging.info(f"RESPONSE TEXT: {res.text}")

    return func.HttpResponse("New user created in Okta")
