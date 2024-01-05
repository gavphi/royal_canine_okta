import sqlalchemy as sa
import pandas as pd
from urllib import parse
import logging
import numpy as np
from sqlalchemy import and_
from core import config

def insert_in_db(df, table, dtype, index, conn, row, identity_value=False):
    """
    This function is designed to create and save a new row in a table only 
    if the desired row does not already exist.

    The pandas DataFrame df must have the same fields as the table we want to
    save the information to.
    """
    # ID doesn't exist, insert the data
    logging.info(f"df: {df.columns}")
    if identity_value:
        conn.exec_driver_sql(
            f"SET IDENTITY_INSERT [{config.sql_config.database}].[dbo].{table} ON"
        )
    try:
        df.loc[[index]].to_sql(
            table,
            schema="dbo",
            con=conn,
            index=False,
            if_exists="append",
            dtype=dtype.sqlalchemy_dtypes,
        )
        logging.info(f"Inserted data for ID")
    except Exception as e:
        logging.info(
            f"Error inserting data for ID. {str(e)}"
        )
    finally:
        if identity_value:
            conn.exec_driver_sql(
                f"SET IDENTITY_INSERT [{config.sql_config.database}].[dbo].{table} OFF")
            
    return conn

def connect_to_db():
    """
    This function establishes the connection to the DB and returns 
    the respective engine.
    """
    string = (
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + config.sql_config.host
        + ";DATABASE="
        + config.sql_config.database
        + ";UID="
        + config.sql_config.username
        + ";PWD="
        + config.sql_config.password
    )

    params = parse.quote_plus(string)

    logging.info(f"Params: {params}")

    return sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

def update_sfmc_table(df, table, dtype, identity_value=False):
    """
    
    """

    logging.info(f"Inserting in DB in {table}...")
    engine = connect_to_db()

    with engine.begin() as conn:
        for index, row in df.iterrows():
         
            query = f"SELECT COUNT(*) FROM {table} WHERE email = '{row['email']}'"
            result = conn.execute(query)
            count = result.scalar()
            
            if count > 0:
                logging.info("******* UPDATE *******")
                logging.info("Inserting on DB column wise")
                
                if identity_value:
                    conn.exec_driver_sql(
                        f"SET IDENTITY_INSERT [{config.sql_config.database}].[dbo].{table} ON"
                    )

                sfmc_table = sa.Table(table, sa.MetaData(), autoload=True, autoload_with=engine)

                upd = sfmc_table.update().where(sfmc_table.c.email == row["email"])

                logging.info(f"upd: {upd}")

                

                dict_ = {"email": row["email"], "name": row["name"], 
                         "surname": row["surname"], "mobilephone": row["mobilephone"],
                         "lng": row["lng"], "countryCode": row["countryCode"], "registry_date": row["registry_date"] }

                logging.info(f"Updating row of {table} with this data: {dict_}")
                val = upd.values(dict_)

                logging.info(f"val: {val}")
                engine.execute(val)

                logging.info(f"Inserted data for ID: {row['email']}")


            else:
                logging.info("******* INSERTING *******")
                conn = insert_in_db(df, table, dtype, index, conn, row, identity_value)

def update_okta_table(df, table, dtype, identity_value=False):
    """
    
    """

    logging.info(f"Inserting in DB in {table}...")
    engine = connect_to_db()

    with engine.begin() as conn:
        for index, row in df.iterrows():
         
            query = f"SELECT COUNT(*) FROM {table} WHERE id = '{row['id']}'"
            result = conn.execute(query)
            count = result.scalar()
            
            if count > 0:
                logging.info("******* UPDATE *******")
                logging.info("Inserting on DB column wise")
                
                if identity_value:
                    conn.exec_driver_sql(
                        f"SET IDENTITY_INSERT [{config.sql_config.database}].[dbo].{table} ON"
                    )

                sfmc_table = sa.Table(table, sa.MetaData(), autoload=True, autoload_with=engine)

                upd = sfmc_table.update().where(sfmc_table.c.id == row["id"])

                logging.info(f"upd: {upd}")

                dict_ = {"email": row["email"], "id": row["id"], 
                         "account_type": row["account_type"], "registry_date": row["registry_date"]}

                logging.info(f"Updating row of {table} with this data: {dict_}")
                val = upd.values(dict_)

                logging.info(f"val: {val}")
                engine.execute(val)

                logging.info(f"Inserted data for ID: {row['email']}")


            else:
                logging.info("******* INSERTING *******")
                conn = insert_in_db(df, table, dtype, index, conn, row, identity_value)

def update_consents_table(df, table, dtype, identity_value=False):
    """
    
    """

    logging.info(f"Inserting in DB in {table}...")
    engine = connect_to_db()

    with engine.begin() as conn:
        for index, row in df.iterrows():
         
            query = f"SELECT COUNT(*) FROM {table} WHERE email = '{row['email']}'"
            result = conn.execute(query)
            count = result.scalar()
            
            if count > 0:
                logging.info("******* UPDATE *******")
                logging.info("Inserting on DB column wise")
                
                if identity_value:
                    conn.exec_driver_sql(
                        f"SET IDENTITY_INSERT [{config.sql_config.database}].[dbo].{table} ON"
                    )

                consents_table = sa.Table(table, sa.MetaData(), autoload=True, autoload_with=engine)

                upd = consents_table.update().where(consents_table.c.email == row["email"])

                logging.info(f"upd: {upd}")

                dict_ = {"email": row["email"], "mars_petcare_consent": row["mars_petcare_consent"], 
                         "rc_mkt_consent": row["rc_mkt_consent"], "data_research_consent": row["data_research_consent"],
                         "rc_tyc_consent": row["rc_tyc_consent"], "last_update": row["last_update"]}

                logging.info(f"Updating row of {table} with this data: {dict_}")
                val = upd.values(dict_)

                logging.info(f"val: {val}")
                engine.execute(val)

                logging.info(f"Inserted data for ID: {row['email']}")


            else:
                logging.info("******* INSERTING *******")
                conn = insert_in_db(df, table, dtype, index, conn, row, identity_value)

def parse_from_sql(query):
    """

    """

    logging.info("Inserting in DB...")
    engine = connect_to_db()

    with engine.begin() as conn:

        result = conn.execute(query)
        row = result.fetchall()

    df = pd.DataFrame(row, columns=result.keys()) if row else pd.DataFrame()

    return df
        
def update_withdrawl(df, table, dtype, identity_value=False):
    """
    
    """

    logging.info(f"Inserting in DB in {table}...")
    engine = connect_to_db()

    with engine.begin() as conn:
        for index, row in df.iterrows():
         
            query = f"SELECT COUNT(*) FROM {table} WHERE email = '{row['email']}'"
            result = conn.execute(query)
            count = result.scalar()
            
            if count > 0:
                logging.info("******* UPDATE *******")
                logging.info("Inserting on DB column wise")
                
                if identity_value:
                    conn.exec_driver_sql(
                        f"SET IDENTITY_INSERT [{config.sql_config.database}].[dbo].{table} ON"
                    )

                consents_table = sa.Table(table, sa.MetaData(), autoload=True, autoload_with=engine)

                upd = consents_table.update().where(consents_table.c.email == row["email"])

                logging.info(f"upd: {upd}")

                dict_ = {"email": row["email"], "mars_petcare_consent": row["mars_petcare_consent"], 
                         "rc_mkt_consent": row["rc_mkt_consent"], "data_research_consent": row["data_research_consent"],
                         "rc_tyc_consent": row["rc_tyc_consent"], "last_update": row["last_update"], "withdrawl": 1}

                logging.info(f"Updating row of {table} with this data: {dict_}")
                val = upd.values(dict_)

                logging.info(f"val: {val}")
                engine.execute(val)

                logging.info(f"Inserted data for ID: {row['email']}")
