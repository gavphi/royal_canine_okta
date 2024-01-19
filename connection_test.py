import pyodbc
from urllib.parse import quote_plus
import sqlalchemy as sa


server = 'localhost'
database = 'RC_Data_Migration'
username = 'sa'
password = 'Xc?H9s6Z3a2]E?1QE3'
driver='{ODBC Driver 17 for SQL Server}'

connection_string = (
        f"mssql+pymssql://{username}:{password}@{server}:{1433}/{database}"
    )
    
print(connection_string)
conn = sa.create_engine(connection_string)
query = f"SELECT COUNT(*) FROM UsersSFMC"

result = conn.execute(query)

print(result)