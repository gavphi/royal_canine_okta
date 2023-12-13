import requests

from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from core import config


def get_token(scopes):
    okta_url = config.okta_config.okta_domain

    client_id = config.okta_config.client_id
    private_key = config.okta_config.private_key
    token_url = f"{okta_url}/oauth2/default/v1/token"

    """
             "user.custom:register",
        "users.profile:read",

        "users.profile:write",

    "users.consent:collect",
        "users.consent:delete",
        "users.consent:read",
    """

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
    print(f"Access Token: {access_token}")
    return access_token
