import requests
import json
from api_functions.credentials import get_token
from core import config
import logging
import pandas as pd
from datetime import datetime

ciam_api_url_dev = config.okta_config.ciam_api_url
okta_url = config.okta_config.okta_domain


def create_user(user, token, account_type="test"
):
    try:
        url = f"{ciam_api_url_dev}/lifecycle/register/offline"
        payload = {
            "callback": okta_url,
            "locale": config.okta_config.locale,
            "send_email": False,
            "profile": {
                "firstName": str(user["name"]),
                "lastName": str(user["surname"]),
                "mobilePhone": str(user["mobilephone"]),
                "email": str(user["email"]),
                "login": str(user["email"]),
                "preferredLanguage": str(user["lng"]),
                "countryCode": str(user["countryCode"]),
                "accountType": account_type,
            },
        }

        logging.info(f"payload: {payload}")

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        res = requests.post(url, data=json.dumps(payload), headers=headers)

        logging.info(f"Status Code: {res.status_code}")
        logging.info(f"Message: {res.text}")

        if res.status_code==200:
            logging.info(f"User added successfully to OKTA")
            return res
        else:
            logging.info(f"Unable to add user to OKTA")
            return res

    except Exception as e:
        logging.warning(f"Unable to add user to OKTA: {e}")
        return None
    
def update_user(
    user,
    token, 
    account_type="test"
):
    url = f"{ciam_api_url_dev}/directory/users/{user['id']}"

    print(url)
    logging.info(f"USER UPDATE: {user}")
    payload = {
        "profile": {
            "firstName": str(user["name"]),
            "lastName": str(user["surname"]),
            "mobilePhone":  str(int(user["mobilephone"])),
            "preferredLanguage": str(user["lng"]),
            "countryCode": str(user["countryCode"]),
            "accountType": account_type,
        },
    }

    print(payload)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    payload_json = json.dumps(payload)
    res = requests.post(url, data=payload_json, headers=headers)

    return res


def get_user(id, token):

    logging.info(f"Getting user information with id: {id}")
    url = f"{ciam_api_url_dev}/directory/users/{id}"

    #print(url)
    #logging.info(f"URL: {url}")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    res = requests.get(url, params = {}, headers=headers)

    #logging.info(res)
    return res

