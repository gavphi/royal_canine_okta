import logging
from api_functions.okta import create_user, get_user, update_user, get_token
from api_functions.utils import load_json
from azure_storage import AzureStorage
import azure.functions as func
from core import config
import pandas as pd
import json
from datetime import datetime, timedelta
from api_functions.db_functions import parse_from_sql, update_okta_table
from api_functions.db_schemas import UsersOKTA_TblSchema
from api_functions.utils import convert_dates
import time

account_type = 'test'

def main(req: func.HttpRequest) -> func.HttpResponse:

    """"
    FIX ME: IMPLEMENT MECANISM TO ONLY INGEST 5 USERS PER MINUTE
    """
    logging.info('Creating new users in OKTA')
    
    azs = AzureStorage(config.azure_config.container_name)

    start_date = "2023-03-29" #datetime.today().strftime('%Y-%m-%d')

    today = datetime.today()
    day_after = today + timedelta(days=1)
    end_date = "2023-03-31" #day_after.strftime('%Y-%m-%d')

    query = f"""SELECT * from UsersSFMC us where registry_date > '{start_date} 00:00:00.000' and registry_date < '{end_date} 00:00:00.000'"""
    users_df = parse_from_sql(query)

    logging.info(f"user_data: {users_df}")
    
    logging.info("Inserting user")

    users_processed = []
    #idx = 0

    #users_df.drop_duplicates(inplace=True)
    #users_df.to_csv("users_df.csv")

    get_user_token = get_token(['users.profile:read'])
    create_user_token = get_token(['user.custom:register'])
    update_user_token = get_token(['users.profile:write'])

    idx = 6
    for index, user_data in users_df.iterrows():

        get_res = get_user(user_data["email"], get_user_token)

        logging.info(f"get_res: {get_res.text}")
        if get_res.status_code == 404 or get_res == None:
            
            logging.warning("User doesn't exist. Creating User.")
            create_res = create_user(user_data, create_user_token)

            if create_res:
                if create_res.status_code == 200:
                    
                    logging.info("User created successfully. Inserting in DB.")
                    id = json.loads(create_res.text)['id']

                    user_data["id"] = id

                    logging.info(id)

                    user_data["account_type"] = account_type
                    
                    user_data["registry_date"] = pd.to_datetime(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))

                    users_processed.append(user_data)

                    #update_okta_table(user.to_frame().T[['id', 'email', 'account_type', 'registry_date']], "UsersOkta", UsersOKTA_TblSchema())
                    
            else:
                logging.info(create_res)
        elif get_res.status_code == 200:
                logging.warning(f"User already exists. Updating fields.")
                
                id = json.loads(get_res.text)['id']
                last_updated = json.loads(get_res.text)['lastUpdated']

                logging.info(f"last_updated: {type(last_updated)}")
                last_updated = convert_dates(last_updated)
                registry_date = convert_dates(user_data['registry_date'].strftime('%Y-%m-%d %H:%M:%S'))

                logging.info(f"Last Updated: {last_updated}")
                logging.info(f"Registry Date: {registry_date}")
                
                if last_updated < registry_date:
                    user_data["id"] = id
                    
                    user_data["account_type"] = account_type
                    upd_res = update_user(user_data, update_user_token)
                
                    if upd_res:
                        if upd_res.status_code == 200:
                            
                            logging.info("Updated successfully. Inserting in DB.")
                            user_data["registry_date"] = pd.to_datetime(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
                            users_processed.append(user_data)

                            #update_okta_table(user_data.to_frame().T[['id', 'email', 'account_type', 'registry_date']], "UsersOkta", UsersOKTA_TblSchema())
        
        if idx > 5:
            time.sleep(60)
    return func.HttpResponse("Users updated in Okta.")
   
    
