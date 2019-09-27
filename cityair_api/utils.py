import pandas as pd
import datetime
import time
import sys


def to_date(date_string):
    if isinstance(date_string, datetime.datetime):
        return date_string
    else:
        return pd.to_datetime(date_string, dayfirst=True, utc=True)


def timeit(method):
    def timed(*args, **kwargs):
        timeit = kwargs.get('timeit', False)
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        if timeit:
            print(f"{te - ts:.2f} seconds took to {method.__name__} of size {sys.getsizeof(result) / 1000:.2f} KB. args were {', '.join(map(str, args[1:]))}. kwargs were: {kwargs}")
        return result

    return timed


def debugit(method):
    def print_request_response(*args, **kwargs):
        obj = args[0]
        body = {"User": getattr(obj, 'user'), "Pwd": getattr(obj, 'psw'), **kwargs}
        url = f"{getattr(obj, 'host_url', None)}/{args[1]}"
        result = method(*args, **kwargs)
        if kwargs.get('debugit'):
            print(f"url: {url}\nrequest_body: {body}\nresponse: {result}")
        return result
    return print_request_response
