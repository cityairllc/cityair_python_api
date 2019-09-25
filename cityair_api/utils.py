import pandas as pd
import datetime


def to_date(date_string):
    if isinstance(date_string, datetime.datetime):
        return date_string.isoformat()
    else:
        return pd.to_datetime(date_string, dayfirst=True, utc=True).isoformat()
