"""
PIPELINE TESTS
"""
import logging
import api_functions.salesforce_connection as sfmc
from api_functions.db_functions import *
import io
from api_functions.db_functions import connect_to_db, parse_from_sql
from api_functions.okta import get_user, get_token
from api_functions.grant_consent import get_consent, granting_consent
import json

"""

#Test ExtractSalesForceDE 


start_date = "2024-01-18" #datetime.today().strftime('%Y-%m-%d')
today = datetime.today()
day_after = today + timedelta(days=1)
end_date = "2024-01-19" #day_after.strftime('%Y-%m-%d')

page = "start_of_life_dogs"
config_json = load_json("config.json")
clave = config_json[page]["clave"]
date_column = config_json[page]["date_column"]

sfmc_token = sfmc.get_token()
data = sfmc.get_data(sfmc_token, start_date, end_date, clave, date_column)

endpoint_count = len(data)

engine = connect_to_db()
with engine.begin() as conn:

    query = f"SELECT COUNT(*) FROM UsersSFMC"

    result = conn.execute(query)

    sfmc_db_count = result.scalar()


if endpoint_count == sfmc_db_count:
    print("SFMC endpoint and the DB have the same count.")
else:
    print("Different count detected!")
    print(f"Count SFMC: {endpoint_count}")
    print(f"Count DB: {sfmc_db_count}")
"""


"""#Test ExtractOktaUser 

df = parse_from_sql(f"SELECT email FROM UsersSFMC")

print(df)


users = []
for index, row in df.iterrows():
    print(row['email'])
    users.append(get_user(row['email']))

print(len(users))"""

df = parse_from_sql(f"SELECT * FROM UsersOkta uo JOIN OneTrustConsent otc ON OTC.email = UO.email")


consents = []
users = []
for index, row in df.iterrows():
    user_id = row['id']
    mars_pet_car_consent = row['mars_petcare_consent']
    rc_mkt_consent = row['rc_mkt_consent']
    data_research_consent = row['data_research_consent']
    rc_tyc_consent = row['rc_tyc_consent']
    consents.append([mars_pet_car_consent, rc_mkt_consent, data_research_consent, rc_tyc_consent])
    users.append(user_id)


total_consents  = []
for user, consent_db in zip(users, consents):
    consent = get_consent(user, get_token("users.consent:read"))
    consent = json.loads(consent.text)['content']
    one_trust_consents = []

    if consent:
        for purpose in consent[0]["Purposes"]:
            one_trust_consents.append(purpose["Id"])

        total_consents.append(one_trust_consents)

print(consents)
print(total_consents)



"""
granting_consent("00u1yv4sw86wcMNYc0h8", [config.consents_config.data_research_consent, config.consents_config.rc_mkt_consent])

res = get_consent("00u1yv4sw86wcMNYc0h8")
print(res.text)
print(len(json.loads(res.text)['content'][0]['Purposes']))
"""
