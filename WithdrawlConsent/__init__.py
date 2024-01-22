import logging
from api_functions.salesforce_connection import get_data, transform_data
from api_functions.grant_consent import withdrawl_consent, get_consent
from api_functions.okta import get_token, get_user
from api_functions.utils import load_json
from azure_storage import AzureStorage
import azure.functions as func
from core import config
import pandas as pd
import json
from api_functions.db_functions import parse_from_sql
from datetime import datetime, timedelta

def main(req: func.HttpRequest) -> func.HttpResponse:
        logging.info('Withdrawl of consents One Trust')

        azs = AzureStorage(config.azure_config.container_name)

        #input_config = req.get_json()
        input_config = {"page": "unsubscribed"}
        page = input_config['page']

        start_date = "2023-09-23" #datetime.today().strftime('%Y-%m-%d')

        today = datetime.today()
        day_after = today + timedelta(days=1)
        end_date = "2023-09-25" #day_after.strftime('%Y-%m-%d')

        query = f"""SELECT otc.email, otc.mars_petcare_consent, otc.rc_mkt_consent, otc.data_research_consent, otc.rc_tyc_consent
                FROM OneTrustConsent otc
                WHERE otc.registry_date > '{start_date} 00:00:00.000' AND otc.registry_date < '{end_date} 00:00:00.000'
                and otc.withdrawl = 1
                """
        
        logging.info(f"query: {query}")
        user_df = parse_from_sql(query)

        withdrawl_token = get_token("users.consent:delete")

        if user_df.empty:
                return func.HttpResponse(f"Users don't exist yet in the system to be withdrawn.", status_code=400)
        else:

                get_user_token = get_token(['users.profile:read'])

                
                for index, user_data in user_df.iterrows():
                                res = get_user(user_data['email'], get_user_token)
                
                                if res.status_code == 200:

                                        id = json.loads(res.text)['id']

                                        logging.info(f"Withdrawl running for id: {id}")
                                        
                                        for purpose in [config.consents_config.mars_petcare_consent, 
                                                        config.consents_config.data_research_consent,
                                                        config.consents_config.rc_mkt_consent,
                                                        config.consents_config.rc_tyc_consent]:
                                                res = withdrawl_consent(id, purpose, withdrawl_token)
                                                if res.status_code == 200:
                                                        logging.info(f"{purpose} withdrawn successful.")
                                                else:
                                                        logging.warning(f"{purpose} withdrawn failed.")
                                else:
                                        logging.warning(f"Withdrawl unsuccessful. User not found in okta.")

                return func.HttpResponse(f"Withdrawl successful", status_code=200)
        
                
                
                        