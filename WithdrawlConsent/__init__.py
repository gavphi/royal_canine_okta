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

        start_date = "2023-06-27" #datetime.today().strftime('%Y-%m-%d')

        today = datetime.today()
        day_after = today + timedelta(days=1)
        end_date = "2023-06-28" #day_after.strftime('%Y-%m-%d')

        query = f"""SELECT us.id, otc.email, otc.mars_petcare_consent, otc.rc_mkt_consent, otc.data_research_consent, otc.rc_tyc_consent
                FROM OneTrustConsent otc
                JOIN UsersOkta us ON us.email = otc.email
                WHERE us.registry_date > '{start_date} 00:00:00.000' AND us.registry_date < '{end_date} 00:00:00.000'
                and otc.withdrawl = 1
                """
        
        logging.info(f"query: {query}")
        user_df = parse_from_sql(query)

        withdrawl_token = get_token("users.consent:delete")

        if user_df.empty:
                return func.HttpResponse(f"Users don't exist yet in the system to be withdrawn.", status_code=400)
        else:

                for index, user_data in user_df.iterrows():
                        
                        if user_data["mars_petcare_consent"] == 1:
                                try:
                                        res = withdrawl_consent(user_data["id"], config.consents_config.mars_petcare_consent, withdrawl_token)
                                        if res.status_code == 200:
                                                logging.info(f"mars_petcare_consent withdrawn successful.")
                                        else:
                                                logging.warning(f"mars_petcare_consent withdrawn failed.")

                                except Exception as e:
                                        logging.error(f"mars_petcare_consent withdrawl was not sucessful.")

                        
                        if user_data["data_research_consent"] == 1:
                                try:    
                                        res = withdrawl_consent(user_data["id"], config.consents_config.data_research_consent, withdrawl_token)
                                        if res.status_code == 200:
                                                logging.info(f"data_research_consent withdrawn successful")
                                except Exception as e:
                                        logging.error(f"data_research_consent withdrawl was not sucessful.")

                        if user_data["rc_mkt_consent"] == 1:
                                try:
                                        res = withdrawl_consent(user_data["id"], config.consents_config.rc_mkt_consent, withdrawl_token)
                                        if res.status_code == 200:
                                                logging.info(f"rc_mkt_consent withdrawn successful")
                                except Exception as e:
                                        logging.error(f"rc_mkt_consent withdrawl was not sucessful.")

                        if user_data["rc_tyc_consent"] == 1:
                                try:
                                        res = withdrawl_consent(user_data["id"], config.consents_config.rc_tyc_consent, withdrawl_token)
                                        if res.status_code == 200:
                                                logging.info(f"rc_tyc_consent withdrawn successful")
                                except Exception as e:
                                        logging.error(f"rc_tyc_consent withdrawl was not sucessful.")

                return func.HttpResponse(f"Withdrawl successful", status_code=200)

                
                        