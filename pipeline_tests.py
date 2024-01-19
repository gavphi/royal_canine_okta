"""
PIPELINE TESTS
"""
import logging
from api_functions.salesforce_connection import *
from api_functions.db_functions import *
import io
from api_functions.db_functions import connect_to_db, parse_from_sql
from api_functions.okta import get_user
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

sfmc_token = get_token()
data = get_data(sfmc_token, start_date, end_date, clave, date_column)

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

"""
Test ExtractOktaUser 
"""
df = parse_from_sql(f"SELECT email FROM UsersSFMC")

print(df)


users = []
for index, row in df.iterrows():
    print(row['email'])
    users.append(get_user(row['email']))

print(len(users))