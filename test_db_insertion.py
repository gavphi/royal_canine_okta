
from api_functions.db_functions import update_sfmc_table
from api_functions.db_schemas import UsersSFMC_TblSchema
import pandas as pd

users_df = pd.read_csv('users.csv')

update_sfmc_table(users_df[["name","surname","email","mobilephone","lng","countryCode", "registry_date", "data_extension", "last_update"]], "UsersSFMC", UsersSFMC_TblSchema())