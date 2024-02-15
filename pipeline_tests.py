"""
PIPELINE TESTS
"""
import logging
import api_functions.salesforce_connection as sfmc
from api_functions.db_functions import *
import io
from api_functions.utils import load_json
from api_functions.db_functions import connect_to_db, parse_from_sql
from api_functions.okta import get_user, get_token
from api_functions.grant_consent import get_consent, granting_consent, withdrawl_consent
import json


class DataVerificationClass():
    def __init__(self, start_date, end_date, page):
        self.start_date = start_date
        self.end_date = end_date
        self.page = page
        self.get_user_token = get_token(['users.profile:read'])
        self.create_user_token = get_token(['user.custom:register'])
        self.update_user_token = get_token(['users.profile:write'])
        self.get_consent_token = get_token("users.consent:read")
        self.withdrawl_consent_token = get_token("users.consent:delete")

    def TestExtractSalesForceDE(self):

        config_json = load_json("config.json")
        clave = config_json[self.page]["clave"]
        date_column = config_json[self.page]["date_column"]

        sfmc_token = sfmc.get_token()
        data = sfmc.get_data(sfmc_token, self.start_date, self.end_date, clave, date_column)

        endpoint_count = len(data)

        engine = connect_to_db()
        with engine.begin() as conn:

            query = f"SELECT COUNT(*) FROM UsersSFMC where registry_date > '{self.start_date} 00:00:00.000' and registry_date < '{self.end_date} 00:00:00.000' and data_extension = '{self.page}'"

            
            result = conn.execute(query)

            self.sfmc_db_count = result.scalar()

        if endpoint_count == self.sfmc_db_count:
            print("SFMC endpoint and the DB have the same count.")
            print(f"Count SFMC: {endpoint_count}")
            print(f"Count DB: {self.sfmc_db_count}")
        else:
            print("Different count detected!")
            print(f"Count SFMC: {endpoint_count}")
            print(f"Count DB: {self.sfmc_db_count}")
        
    def TestCreateOktaUser(self):
        query = f"SELECT email FROM UsersSFMC where registry_date > '{self.start_date} 00:00:00.000' and registry_date < '{self.end_date} 00:00:00.000'"

        df = parse_from_sql(query)
        
        get_user_token = get_token(['users.profile:read'])

        self.okta_users = []
        res = None
        for index, row in df.iterrows():
            res = get_user(row['email'], get_user_token)
            id = json.loads(res.text).get("id")
            if id:
                self.okta_users.append(id)

        if len(self.okta_users) == self.sfmc_db_count:
            print("Okta and the SFMc have the same count.")
            print(f"Count Okta: {len(self.okta_users)}")
            print(f"Count SFMC DB: {self.sfmc_db_count}")
            
            if res:
                print(f"Okta Response: {res.text}")
        else:
            print("Different count detected!")
            print(f"Count Okta: {len(self.okta_users)}")
            print(f"Count SFMC DB: {self.sfmc_db_count}")

    def TestCreateOktaConsent(self):

        df = parse_from_sql(f"SELECT * FROM OneTrustConsent otc where otc.registry_date > '{self.start_date} 00:00:00.000' and otc.registry_date < '{self.end_date} 00:00:00.000'")

        consents = []
        for index, row in df.iterrows():
            mars_pet_car_consent = row['mars_petcare_consent']
            rc_mkt_consent = row['rc_mkt_consent']
            data_research_consent = row['data_research_consent']
            rc_tyc_consent = row['rc_tyc_consent']
            consents.append([mars_pet_car_consent, rc_mkt_consent, data_research_consent, rc_tyc_consent])

        print(f"Stored consents: {consents}")
        total_consents  = []

        for user_id in self.okta_users:

            consent = get_consent(user_id, self.get_consent_token)
            
            consent = json.loads(consent.text)['content']

            print(f"Consent: {consent}")
            one_trust_consents = []

            if consent:
                for purpose in consent[0]["Purposes"]:
                    one_trust_consents.append(purpose["Id"])

                total_consents.append(one_trust_consents)


        for consent, total_consent in zip(consents, total_consents):
            if len(consent) == len(total_consent):
                print(f"Consents count match for user {id}")
                print(consents)
                print(total_consents)   
            else:
                print(f"Consents count don't match. Some are missing in OneTrust for user {id}")
                print(consents)
                print(total_consents)

    def TestWithdrawl(self, user_id):
        print(user_id)
        consent = get_consent(user_id, self.get_consent_token)
        print(f"Consent before Withdrawl: {consent.text}")

        withdrawl_consent(user_id, config.consents_config.rc_mkt_consent, self.withdrawl_consent_token)

        consent = get_consent(user_id, self.get_consent_token)
        print(f"Consent after Withdrawl: {consent.text}")


dv = DataVerificationClass("2023-03-29", "2023-03-31", "start_of_life_kittens")

#dv.TestExtractSalesForceDE()

#dv.TestCreateOktaUser()

#dv.TestCreateOktaConsent()

dv.TestWithdrawl("00u1yy5owe6avPuOm0h8")