import requests
import pandas as pd
from io import StringIO
import json
from core import config
from api_functions.utils import load_json
import logging
subdomain = config.sfmc_config.subdomain
landing_page_json = load_json("landing_pages.json")


def get_token():
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

    return token


def get_data(token, initial_date, end_date, landind, data_column):
    url = f"https://{subdomain}.rest.marketingcloudapis.com/data/v1/customobjectdata/key/{landind}/rowset?$filter={data_column}%20between%20'{initial_date}T00:00:00'%20and%20'{end_date}T00:00:00'"

    headers = {'Authorization': f'Bearer {token}'
    }

    print("URL: ", url)

    csv_response = requests.get(url, headers=headers)

    print(csv_response.json())
    with open('data.json', 'w') as f:
        json.dump(csv_response.json(), f)

    logging.info(csv_response.status_code)
    return csv_response.json()["items"]

def parse_dictionary(user_value, lp_name, field):
    field_name = landing_page_json[lp_name][field]
    if field_name in user_value:
        value = user_value[field_name]
    else:
        value = None

    return value

def transform_data(data, lp_name):

    users = []
    for user in data:
        
        user_value = user["values"]

        name = parse_dictionary(user_value, lp_name, "name")
        surname = parse_dictionary(user_value, lp_name, "surname")
        email = parse_dictionary(user_value, lp_name, "email")
        mobilephone = parse_dictionary(user_value, lp_name, "mobilephone")
        language = parse_dictionary(user_value, lp_name, "language")
        countryCode = parse_dictionary(user_value, lp_name, "countryCode")
        accountType = "test"

        user_data = {
            "firstName": name,
            "lastName": surname,
            "email": email,
            "mobilePhone": mobilephone,
            "preferredLanguage": language,
            "countryCode": countryCode,
            "accountType": accountType
        }
        
        users.append(user_data)

    return users

def post_process_name_surname(user_data):

    users = []
    for user in user_data:

        full_name = user["firstName"].split()

        user["firstName"] = full_name[0]
        user["lastName"] = ' '.join(full_name[1::])

        users.append(user)

    return users