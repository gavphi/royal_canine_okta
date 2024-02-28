import json
from azure_storage import AzureStorage
from datetime import datetime
from dateutil import parser
import pytz
import logging
import io

def load_json(filename):
    f = open(filename)
    
    data = json.load(f)

    return data


def convert_dates(date_str):
    
    date_obj_utc = parser.parse(date_str)

    # Convert to Barcelona time
    barcelona_timezone = pytz.timezone('Europe/Madrid')
    date_obj_barcelona = date_obj_utc.astimezone(barcelona_timezone)

    # Format the datetime object as needed
    formatted_date = date_obj_barcelona.strftime('%Y-%m-%d %H:%M:%S')

    return formatted_date


def setup_logger():
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger()
    logger.addHandler(handler)

    return logger, log_stream