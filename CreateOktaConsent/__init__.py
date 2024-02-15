import logging
from api_functions.salesforce_connection import get_data, transform_data
from api_functions.grant_consent import granting_consent
from api_functions.okta import get_user
from api_functions.credentials import get_token
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

    start_date = "2023-03-29" #datetime.today().strftime('%Y-%m-%d')

    today = datetime.today()
    day_after = today + timedelta(days=1)
    end_date = "2023-03-31" #day_after.strftime('%Y-%m-%d')

    query = f"""SELECT us.email, otc.mars_petcare_consent, otc.rc_mkt_consent, otc.data_research_consent, otc.rc_tyc_consent from UsersSFMC us JOIN OneTrustConsent otc ON us.email = otc.email where us.registry_date > '{start_date} 00:00:00.000' and us.registry_date < '{end_date} 00:00:00.000' and otc.withdrawl = 0"""
    logging.info(query)
    users_df = parse_from_sql(query)

    users_df.to_csv("users_df.csv")
    get_user_token = get_token(['users.profile:read'])

    for index, user_data in users_df.iterrows():

        id = json.loads(get_user(user_data['email'], get_user_token).text)['id']

        #try:
        logging.info(f"{id} {user_data['email']}: mars_petcare_consent: {user_data['mars_petcare_consent']}")
        logging.info(f"{id} {user_data['email']}: rc_mkt_consent: {user_data['rc_mkt_consent']}")
        logging.info(f"{id} {user_data['email']}: data_research_consent: {user_data['data_research_consent']}")
        logging.info(f"{id} {user_data['email']}: rc_tyc_consent: {user_data['rc_tyc_consent']}")
        
        purposes = []
        if int(user_data["mars_petcare_consent"]) == 1:
            purposes.append(config.consents_config.mars_petcare_consent)

        if int(user_data["rc_mkt_consent"]) == 1:
            purposes.append(config.consents_config.rc_mkt_consent)

        if int(user_data["data_research_consent"]) == 1:
            purposes.append(config.consents_config.data_research_consent)
        
        if int(user_data["rc_tyc_consent"]) == 1:
            purposes.append(config.consents_config.rc_tyc_consent)

        logging.info(f"purposes: {purposes}")
        logging.info(f"id: {id}")
        res = granting_consent(id, purposes)
        logging.info(f"rc_tyc_consent: {res.status_code}")

        """except:
            logging.warning("Consents not created.")"""


    return func.HttpResponse("Consent Created in OneTrust")
