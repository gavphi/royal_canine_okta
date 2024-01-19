import sqlalchemy as sa
import pandas as pd
from urllib import parse
import logging
import numpy as np
from sqlalchemy import bindparam, exc
from core import config
from datetime import datetime
from api_functions.db_schemas import *

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
    server = 'localhost'
    database = 'RC_Data_Migration'
    username = 'sa'
    password = 'Xc?H9s6Z3a2]E?1QE3'
    driver='{ODBC Driver 17 for SQL Server}'

    connection_string = (
            f"mssql+pymssql://{username}:{password}@{server}:{1433}/{database}"
        )
    
    return sa.create_engine(connection_string)

def update_sfmc_table(df, table, dtype, identity_value=False):
    """
    
    """

    logging.info(f"Inserting in DB in {table}...")
    engine = connect_to_db()

    with engine.begin() as conn:
        for index, row in df.iterrows():
            
            logging.info(f"email type: {type(row['email'])}")
            query = f"SELECT COUNT(*) FROM {table} WHERE email = '{row['email']}'"

            logging.info(f"Query: {query}")
            result = conn.execute(query)

            count = result.scalar()

            logging.info(f"Count: {count}")
            
            if count > 0:

                logging.info("******* UPDATE *******")
                logging.info("Updating on DB column wise.")
                logging.info(f"Email: {row['email']}")

                
                query = f"SELECT * FROM {table} WHERE email = '{row['email']}'"
                df_db = parse_from_sql(query)
                
                
                if df_db.registry_date[0] <= pd.to_datetime(row['registry_date']):
                    dict_ = {}
                    
                    print("keys", df.keys())
                    for key in df.keys():
                        
                        if key == 'registry_date' or key == 'last_update':
                            value = pd.to_datetime(row[key])
                        else:
                            value = row[key]
                        dict_[key] = value

                    dict_ = {k: v if pd.notnull(v) else None for k, v in dict_.items()}

                    sfmc_table = sa.Table(table, sa.MetaData(), autoload=True, autoload_with=engine)
                    
                    update_values = {}
                    for column_name, column_value in dict_.items():
                        update_values[sfmc_table.c[column_name]] = bindparam(column_name, value=column_value, type_=UsersSFMC_TblSchema().sqlalchemy_dtypes[column_name])

                    upd = sfmc_table.update().where(sfmc_table.c.email == row["email"])

                    logging.info(f"Updating row of {table} with this data: {dict_}")
                    val = upd.values(update_values)

                    engine.execute(val)

                    logging.info(f"Updated data for email: {row['email']}")

                
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

            conn = insert_in_db(df, table, dtype, index, conn, row, identity_value)
            
            if count > 0:
                logging.info("******* UPDATE *******")
                logging.info("Updating on DB column wise.")
                logging.info(f"Email: {row['email']}")

                
                query = f"SELECT * FROM {table} WHERE email = '{row['email']}'"
                df_db = parse_from_sql(query)
                
                
                if df_db.registry_date[0] <= pd.to_datetime(row['registry_date']):
                    dict_ = {}
                    
                    print("keys", df.keys())
                    for key in df.keys():
                        
                        if key == 'registry_date' or key == 'last_update':
                            value = pd.to_datetime(row[key])
                        else:
                            value = row[key]
                        dict_[key] = value

                    dict_ = {k: v if pd.notnull(v) else None for k, v in dict_.items()}

                    sfmc_table = sa.Table(table, sa.MetaData(), autoload=True, autoload_with=engine)
                    
                    update_values = {}
                    for column_name, column_value in dict_.items():
                        update_values[sfmc_table.c[column_name]] = bindparam(column_name, value=column_value, type_=UsersSFMC_TblSchema().sqlalchemy_dtypes[column_name])

                    upd = sfmc_table.update().where(sfmc_table.c.email == row["email"])

                    logging.info(f"Updating row of {table} with this data: {dict_}")
                    val = upd.values(update_values)

                    engine.execute(val)

                    logging.info(f"Updated data for email: {row['email']}")

                
            else:
                logging.info("******* INSERTING *******")
                conn = insert_in_db(df, table, dtype, index, conn, row, identity_value)

def update_consents_table(df, table, dtype, withdrawl=False):
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

                query = f"SELECT * FROM {table} WHERE email = '{row['email']}'"
                df = parse_from_sql(query)
                
                if withdrawl:
                    dict_ = {"email": row["email"], "withdrawl": 1}
                else:
                    dict_ = {}
                
                    for key in df.keys():
                        logging.info(f"KEY: {key}")
                        logging.info(f"value: {row[key]}")
                        logging.info(f"value df: {df[key].values[0]}")
                        if df[key].values[0]:
                            logging.info(f"type: {type(df[key].values[0])}")
                            if key == 'registry_date' or key == 'last_update':
                                value = row[key]
                            else:
                                value = df[key].values[0]
                            dict_[key] = value
                        else:
                            value = row[key]

                            logging.info(f"type value: {type(value)}")
                            dict_[key] = value


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
                conn = insert_in_db(df, table, dtype, index, conn, row, False)

def parse_from_sql(query):
    """

    """

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
