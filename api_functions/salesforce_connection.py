import requests
import pandas as pd
from io import StringIO
import json
from core import config
from api_functions.utils import load_json
import logging
import urllib.parse
from datetime import datetime, timedelta
from dateutil.relativedelta import *

subdomain = config.sfmc_config.subdomain
landing_page_json = load_json("config.json")


def get_token():
    logging.info("Generating SFMC token...")

    try:
        headers = {"Content-Type": "application/json"}
        

        url = f"https://{subdomain}.auth.marketingcloudapis.com/v2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": "0stvbuuuy62dkb7eafu41217",
            "client_secret": "qiFPTp8wBvCHMCxdQaCDYoyP",
            "account_id": "534006563"
            }

        res = requests.post(url, data=json.dumps(payload), headers=headers)

        response_json = res.json()

        token = response_json["access_token"]

        logging.info("Token generated.")
    except Exception as e:
        logging.error(f"Token not generated: {e}")
    return token

def get_month_intervals(start_date_str, end_date_str):

    print(start_date_str, end_date_str)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    monthly_intervals = []
    
    current_date = start_date
    while current_date <= end_date:
        monthly_intervals.append(str(current_date.strftime('%Y-%m-%d')))
        current_date += relativedelta(months=+1)
        print(current_date)

    return monthly_intervals

def get_all_data(token, landind):
    
    page=1
    all_data = []
    items = 1
    while items != []:
        url = f"https://{subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/{landind}/rowset?$page={page}"
        
        headers = {'Authorization': f'Bearer {token}'
        }

        logging.info(f"URL: {url}")
            
        csv_response = requests.get(url, headers=headers)
        
        items = csv_response.json().get("items", [])

        page+=1

        if items != []:
            all_data.append(items)

    return [item for sublist in all_data for item in sublist]


def get_data(token, initial_date, end_date, landind, date_column):

    date_column_encoded = date_column.replace(" ", "+")

    url = f"https://{subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/{landind}/rowset?$filter={date_column_encoded}%20between%20'{initial_date}T00:00:00'%20and%20'{end_date}T00:00:00'"

    headers = {'Authorization': f'Bearer {token}'
    }

    logging.info(f"URL: {url}")
        
    csv_response = requests.get(url, headers=headers)

    
    logging.info(csv_response.status_code)
    
    #try:
    return csv_response.json()["items"]
    """except:
        return []"""

def parse_dictionary(user_value, lp_name, field):
    field_name = landing_page_json[lp_name][field]

    if field_name in user_value.keys():
        value = user_value[field_name]
    else:
        value = None

    return value

def transform_data(data, lp_name, wd):

    users = []

    for user in data:
        
        user_value = user["values"]
        user_keys = user["keys"]

        user_value = {**user_value, **user_keys}

        name = parse_dictionary(user_value, lp_name, "name")
        surname = parse_dictionary(user_value, lp_name, "surname")
        email = parse_dictionary(user_value, lp_name, "email")
        mobilephone = parse_dictionary(user_value, lp_name, "mobilephone")
        language = "es-ES"
        countryCode = "MX"

        registry_date = parse_dictionary(user_value, lp_name, "date_column")
        
        mars_petcare_consent = parse_dictionary(user_value, lp_name, "mars_petcare_consent")
        rc_mkt_consent = parse_dictionary(user_value, lp_name, "rc_mkt_consent")
        data_research_consent = parse_dictionary(user_value, lp_name, "data_research_consent")
        rc_tyc_consent = parse_dictionary(user_value, lp_name, "rc_tyc_consent")

        user_data = {
            "name": name,
            "surname": surname,
            "email": email,
            "mobilephone": mobilephone,
            "lng": language,
            "countryCode": countryCode,
            "registry_date": registry_date,
            "mars_petcare_consent": mars_petcare_consent,
            "rc_mkt_consent": rc_mkt_consent,
            "data_research_consent": data_research_consent,
            "rc_tyc_consent": rc_tyc_consent,
            "withdrawl": wd
        }
        
        users.append(user_data)

    return users

def post_process_name_surname(user_data):

    users = []
    for user in user_data:

        full_name = user["name"].split()

        user["name"] = full_name[0]
        user["surname"] = ' '.join(full_name[1::])

        users.append(user)

    return users

def prepare_df(users, page):
    users_df = pd.DataFrame(data=users)
    users_df["last_update"] = pd.to_datetime(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    users_df['registry_date'] = users_df['registry_date']

    bool_map = {'True': 1, 'False': 0}

    users_df["mars_petcare_consent"] = users_df['mars_petcare_consent'].map(bool_map).fillna(users_df['mars_petcare_consent'])
    users_df["rc_mkt_consent"] = users_df['rc_mkt_consent'].map(bool_map).fillna(users_df['rc_mkt_consent'])
    users_df["data_research_consent"] = users_df['data_research_consent'].map(bool_map).fillna(users_df['data_research_consent'])
    users_df["rc_tyc_consent"] = users_df['rc_tyc_consent'].map(bool_map).fillna(users_df['rc_tyc_consent'])
    users_df["withdrawl"] = users_df['withdrawl'].map(bool_map).fillna(users_df['withdrawl'])
    users_df["data_extension"] = page

    #users_df = users_df.drop_duplicates(subset=['email'])

    return users_df
def transform_withdrawl_data(data, lp_name="unsubscribed"):

    users = []
    for user in data:
        
        user_value = user["values"]
        user_keys = user["keys"]

        user_value = {**user_value, **user_keys}

        logging.info(f"KEYS: {user_value.keys()}")
        logging.info(f"user_value: {user_value}")
        email = parse_dictionary(user_value, lp_name, "email")
        registry_date = parse_dictionary(user_value, lp_name, "date_column")
        
        user_data = {
            "email": email,
            "last_update": pd.to_datetime(datetime.today().strftime('%Y-%m-%d %H:%M:%S')),
            "withdrawl": 1,
            "registry_date": registry_date
        }
        
        users.append(user_data)

    

    logging.info(f"USERS: {users}")
    return pd.DataFrame(data=users)