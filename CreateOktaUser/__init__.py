import logging
from api_functions.salesforce_connection import get_token, get_data, transform_data
from api_functions.okta import create_user
from api_functions.utils import load_json
import json

import azure.functions as func
import io


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    input_config = req.get_json()

    landing_page_json = load_json("landing_pages.json")

    sfmc_token = get_token()

    clave = landing_page_json["calculadora"]["clave"]

    data = get_data(sfmc_token, "2023-12-07", "2023-12-08", clave)

    users = transform_data(data, 'calculadora')

    with open('users.json', 'w') as f:
        json.dump(users, f)

    user_test = users[1]

    res = create_user(user_test)

    print(res.status_code)
    print(res.text)

    return func.HttpResponse("New user created in Okta")
