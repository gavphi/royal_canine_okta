import requests
import json
from api_functions.credentials import get_token
from core import config

ciam_api_url_dev = config.okta_config.ciam_api_url
okta_url = config.okta_config.okta_domain


def granting_consent(id, purpose_id):
    url = f"{ciam_api_url_dev}/consent/users/{id}"
    payload = {
        "collection_point_guid": "e19dba07-71a0-4e8e-8d2a-b889c55f9f41",
        "collection_point_version": 1,
        "purposes": [
            {"Id": purpose_id},
        ],
    }

    print(f"payload: {payload}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token(['user.consent:collect'])}",
    }

    res = requests.post(url, data=json.dumps(payload), headers=headers)

    return res


def withdrawl_consent(user_id, purpose_id):

    url = f"{ciam_api_url_dev}/consent/users/{user_id}/purposes/{purpose_id}"

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {get_token("user.consent:delete")}'
    }

    response = requests.delete(url, headers=headers)

    return response

def get_consent(user_id):

    url = f"{ciam_api_url_dev}/consent/users/{user_id}"

    data = {}  

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {get_token("user.consent:read")}'
    }
    
    response = requests.get(url, headers=headers, json=data)

    return response
