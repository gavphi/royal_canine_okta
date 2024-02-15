import requests
import json
from api_functions.credentials import get_token
from core import config

ciam_api_url_dev = config.okta_config.ciam_api_url
okta_url = config.okta_config.okta_domain


def granting_consent(id, purposes, token):
    url = f"{ciam_api_url_dev}/consent/users/{id}"

    print(url)

    purposes_ids = []
    for purpose in purposes:
        purposes_ids.append( {"Id": purpose},)
        
    payload = {
        "collection_point_guid": "e19dba07-71a0-4e8e-8d2a-b889c55f9f41",
        "collection_point_version": 1,
        "purposes": purposes_ids
    }

    print(f"payload: {payload}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    res = requests.post(url, data=json.dumps(payload), headers=headers)

    print(res)
    return res


def withdrawl_consent(user_id, purpose_id, token):

    url = f"{ciam_api_url_dev}/consent/users/{user_id}/purposes/{purpose_id}"

    print(url)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.delete(url, headers=headers)

    return response

def get_consent(user_id, token):

    url = f"{ciam_api_url_dev}/consent/users/{user_id}"
    print(url)
    data = {}  

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    response = requests.get(url, headers=headers, json=data)

    #print(response.text)
    return response
