import requests
import json
from api_functions.credentials import get_token
from core import config
import logging

ciam_api_url_dev = config.okta_config.ciam_api_url
okta_url = config.okta_config.okta_domain


def create_user(user, account_type="test"
):

    print(f"User: {user}")
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

    print(f"payload: {payload}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token(['user.custom:register'])}",
    }

    res = requests.post(url, data=json.dumps(payload), headers=headers)

    print(f"Status Code: {res.status_code}")
    print(f"Error Message: {res.text}")
    if res.status_code==200:
        print(f"User added successfully to OKTA")
    return res


def update_user(
    user,
    account_type="test"
):
    url = f"{ciam_api_url_dev}/directory/users/{str(user['id'])}"

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

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token(['user.profile:write'])}",
    }

    payload_json = json.dumps(payload)
    res = requests.post(url, data=payload_json, headers=headers)

    return res


def get_user(id):

    logging.info(f"Getting user information with id: {id}")
    url = f"{ciam_api_url_dev}/directory/users/{id}"

    logging.info(f"URL: {url}")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token(['user.profile:read'])}",
    }

    res = requests.get(url, headers=headers)

    logging.info(res)
    return res


