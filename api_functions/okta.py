import requests
import json
from credentials import get_token
from core import config

ciam_api_url_dev = config.okta_config.ciam_api_url
okta_url = config.okta_config.okta_domain


def create_user(user, account_type="test"
):

    print(f"User: {user.keys()}")
    url = f"{ciam_api_url_dev}/lifecycle/register/offline"
    payload = {
        "callback": okta_url,
        "locale": config.okta_config.locale,
        "send_email": False,
        "profile": {
            "firstName": str(user["firstName"]),
            "lastName": str(user["lastName"]),
            "mobilePhone": str(user["mobilePhone"]),
            "email": str(user["email"]),
            "login": str(user["email"]),
            "preferredLanguage": str(user["preferredLanguage"]),
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

    return res


def update_user(
    id,
    first_name,
    lastName,
    mobilePhone,
    email,
    login,
    language,
    cc,
    account_type="test",
):
    url = f"{ciam_api_url_dev}/directory/users/{id}"
    payload = {
        "callback": okta_url,
        "locale": config.okta_config.locale,
        "send_email": False,
        "profile": {
            "firstName": first_name,
            "lastName": lastName,
            "mobilePhone": mobilePhone,
            "email": email,
            "login": login,
            "preferredLanguage": language,
            "countryCode": cc,
            "accountType": account_type,
        },
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token(['user.profile:write'])}",
    }

    res = requests.post(url, data=json.dumps(payload), headers=headers)

    return res


def get_user(id):
    url = f"{ciam_api_url_dev}/directory/users/{id}"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token(['user.profile:read'])}",
    }

    res = requests.post(url, headers=headers)

    print(res.text)
    return res


"""res = create_user(
    "Alexandra",
    "Soares",
    "000000000",
    "testingmario@gmail.com",
    "testingmario@gmail.com",
    "Spanish",
    "311",
    "test",
)

print(res.text)

res = update_user(
    "00u1xi5rv44tVqOeg0h8"
    "Marilia",
    "Soares",
    "000000200",
    "testingmario@gmail.com",
    "testingmario@gmail.com",
    "Spanish",
    "311",
    "test",
)

print(res.text)

"""

"""res = get_user("00u1xi5rv44tVqOeg0h8")
print(res.text)"""
