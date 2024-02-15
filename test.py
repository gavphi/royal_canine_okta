from api_functions.okta import create_user, update_user, get_user
from api_functions.grant_consent import *
from core import config
import sqlalchemy as sa
import pandas as pd
from urllib import parse
import logging
import numpy as np
from sqlalchemy import bindparam, exc
from core import config
from datetime import datetime
import pyodbc
import urllib.parse

class TestEndpoints():
    def __init__(self, user_id, user):
        self.user_id = user_id,
        self.user = user
        
    def CreateUser(self):

        self.create_user_token = get_token(['user.custom:register'])

        print(self.create_user_token)
        res = create_user(
            self.user, self.create_user_token
        )

        print(res.text)
        return res
    
    def UpdateUser(self):

        self.update_user_token = get_token(['users.profile:write'])
        res = update_user(self.user)

        print(res.text)

        return res

    def GetUser(self):
        
        self.get_user_token = get_token(['users.profile:read'])

        print(self.get_user_token)
        res = get_user(self.user_id[0], self.get_user_token)
        print(res.text)

        return res

    def GrantConsent(self):
        self.grant_consent_token = get_token(['users.consent:collect'])
        res = granting_consent(self.user_id[0], [config.consents_config.data_research_consent, config.consents_config.mars_petcare_consent, 
                                              config.consents_config.rc_mkt_consent, config.consents_config.rc_tyc_consent], self.grant_consent_token)
        print(res.text)
        return res

    def WithdrawlConsent(self):
    
        res = withdrawl_consent(self.user_id, config.consents_config.data_research_consent)
        print(res.text)
        return res

    def GetConsent(self):
        
        self.get_consent_token = get_token("users.consent:read")
        res = get_consent(self.user_id[0], self.get_consent_token)
        print(res.text)
        return res

user_id = "00u1ztwvhurN6EC4E0h8"


create_user_payload = {"name": "Alejandra",
            "surname": "Reynoso",
            "mobilephone": "",
            "email": "areynososanson@gmail.com",
            "login": "areynososanson@gmail.com",
            "lng": "es-Es",
            "countryCode": "MX",}


end = TestEndpoints(user_id, create_user_payload)

#res = end.CreateUser() # Works
#res = end.GetUser()  # "Missing required scope"

#print(res.status_code)
#res = end.GrantConsent() # user id mismatch
#res = end.GetConsent() # "Token user id mismatch"
#res = end.WithdrawlConsent()
#res = TestEndpoints(user_id, update_user_payload).UpdateUser() # "Token user id mismatch"

#res = end.CreateUser() # Works

#print(res.text)

#res = end.GrantConsent() # user id mismatch

res = end.GetConsent() # "Token user id mismatch"


