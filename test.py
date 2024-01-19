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

        res = create_user(
            self.user
        )

        print(res.text)
        return res
    
    def UpdateUser(self):

        res = update_user(self.user)

        print(res.text)

        return res

    def GetUser(self):

        res = get_user(self.user_id)
        print(res.text)

        return res

    def GrantConsent(self):
    
        res = granting_consent(self.user_id, config.consents_config.data_research_consent)
        print(res.text)
        return res

    def WithdrawlConsent(self):
    
        res = withdrawl_consent(self.user_id, config.consents_config.data_research_consent)
        print(res.text)
        return res

    def GetConsent(self):
    
        res = get_consent(self.user_id)
        print(res.text)
        return res

user_id = "00u1ykhpbimKT7te20h8"

update_user_payload = { "id": "00u1yg1ggar8YGnlG0h8", 
            "name": "Margarida",
            "surname": "Fernandes",
            "mobilephone": "000000200",
            "lng": "es-MX",
            "countryCode": "MX"}

create_user_payload = {"name": "Margarida",
            "surname": "Fernandes",
            "mobilephone": "000000000",
            "email": "magg.e.music.project@gmail.com",
            "login": "magg.e.music.project@gmail.com",
            "lng": "Spanish",
            "countryCode": "311",}


end = TestEndpoints(user_id, create_user_payload)

#res = end.CreateUser() # Works
#res = end.GetUser()  # "Missing required scope"

#print(res.status_code)
#res = end.GrantConsent() # user id mismatch
#res = end.GetConsent() # "Token user id mismatch"
#res = end.WithdrawlConsent()
#res = TestEndpoints(user_id, update_user_payload).UpdateUser() # "Token user id mismatch"


