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

from datetime import datetime, timedelta

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Creating consent to new users in OKTA')

    """
    FIX ME: 
    - include loop to create consend per user
    - include retry
    - include try except
    """

    start_date = "2024-01-18" #datetime.today().strftime('%Y-%m-%d')

    today = datetime.today()
    day_after = today + timedelta(days=1)
    end_date = "2024-01-19" #day_after.strftime('%Y-%m-%d')


    query = f""" SELECT otc.*, uo.id
            FROM UsersSFMC us
            JOIN OneTrustConsent otc ON otc.email = us.email
            JOIN UsersOkta uo ON uo.email = us.email 
            WHERE us.registry_date > '{start_date} 00:00:00.000' AND us.registry_date < '{end_date} 00:00:00.000'
            and otc.withdrawl = 0
            """
    
    users_df = parse_from_sql(query)

    logging.info(f"users_df: {users_df}")

    for index, user_data in users_df.iterrows():

        logging.info(f"consent: {user_data['mars_petcare_consent']}")
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
