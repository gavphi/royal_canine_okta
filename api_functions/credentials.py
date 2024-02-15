import requests
from azure_storage import AzureStorage
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from core import config
import requests
import jwt
import pandas as pd
from datetime import datetime, timedelta
import time

def get_token(scopes):
    # Check if token is already stored
    stored_token = load_token(scopes)

    if not stored_token.empty and not is_token_expired(stored_token)[0]:
        print("Using the same token.")
        return stored_token["access_token"][0]
    else:
        print("Token expired. Generating new token.")
        return generate_token(scopes)
    
def generate_token(scopes):
    okta_url = config.okta_config.okta_domain

    client_id = config.okta_config.client_id
    private_key = config.okta_config.private_key
    token_url = f"{okta_url}/oauth2/default/v1/token"

    # Prepare the OAuth2Session with client credentials
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)

    # Request the access token using private_key_jwt auth method
    token = oauth.fetch_token(
        token_url=token_url,
        client_id=client_id,
        client_secret=private_key,
        scope=scopes,
    )

    # Access token
    access_token = token["access_token"]

    store_token_safely(token, scopes)

    return access_token

def store_token_safely(token, scopes):

    azs = AzureStorage(config.azure_config.container_name)

    token_df = [token]
    azs.upload_blob_df(pd.DataFrame(data=token_df), f"tokens/{scopes}.csv")

def load_token(scopes):

    azs = AzureStorage(config.azure_config.container_name)

    df = pd.DataFrame()
    if azs.blob_exists(f"tokens/{scopes}.csv"):

        df = azs.download_blob_df(f"tokens/{scopes}.csv")

        print(df)
        return df
    else:
        return df
            

def is_token_expired(token):
    # Implement logic to check if the token has expired
    current_time = time.time()
    return current_time >= token.get("expires_at", 0)