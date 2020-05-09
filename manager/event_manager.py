from logger import logger
from datetime import datetime, date

def get_date(date_text):
    try:
        return datetime.strptime(date_text, "%Y-%m-%dT%H:%M:%SZ")
        # "%Y-%m-%dT%H:%M:%S")
        # "yyyy-MM-dd'T'HH:mm:ss")
    except Exception as err:
        raise err

def parse_event_date(event):
    current_dateobj = get_date(event["time"])
    start_dateobj = date(current_dateobj.year,1,1)
    return current_dateobj, start_dateobj