import requests
import json
from credentials import get_token
from core import config

ciam_api_url_dev = config.okta_config.ciam_api_url
okta_url = config.okta_config.okta_domain


def granting_consent(id):
    url = f"{ciam_api_url_dev}/consent/users/{id}"
    payload = {
        "collection_point_guid": "e19dba07-71a0-4e8e-8d2a-b889c55f9f41",
        "collection_point_version": 1,
        "purposes": [
            {"Id": "3b1b7e10-883e-48e0-a432-ebd3c4a488ab"},
        ],
    }

    print(f"payload: {payload}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token(['user.consent:collect'])}",
    }

    res = requests.post(url, data=json.dumps(payload), headers=headers)

    return res


res = granting_consent("00u1xi5rv44tVqOeg0h8")
print(res.text)
