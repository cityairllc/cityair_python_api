import pandas as pd
import datetime
import time
import sys
from functools import wraps
from typing import List
from sys import getsizeof
from collections import OrderedDict
"""
TODO

docstrings refactor

"""



RIGHT_PARAMS_NAMES = {'FlagPs220': '220', 'RecvDate': 'RecieveDate',
                      'SendDate': 'date', 'Temperature': 'T',
                      'Humidity': 'RH', 'Pressure': 'P',
                      'ChildDevices': 'children', 'DataDeliveryPeriodSec': 'delivery_period',
                      'Description': 'description',
                      'DeviceCheckInfos': 'check_infos', 'DeviceFirstDate': 'first_packet_date', 'DeviceIMEI': 'IMEI',
                      'DeviceIMSI': 'IMSI',
                      'DeviceLastDate': 'last_packet_date', 'DeviceLastGeo': 'coordinates', 'DeviceName': 'name',
                      'FlagBatLow': 'is_bat_low', 'DeviceId': 'id',
                      'IsOffline': 'is_offline', 'SerialNumber': 'serial_number',
                      'SoftwareVersion': 'software', 'SourceType': 'type', 'TagIds': 'tags', 'MoId': 'id',
                      'Name': 'name', 'PublishName': 'publish_name', 'PublishNameRu': 'publish_name_ru',
                      'ManualDeviceLinks': 'devices_manual', 'DeviceLink': 'devices_auto', 'GmtOffset': 'gmt_offset',
                      'DotItem': 'coordinates',
                      'Latitude': 'latitude', 'Longitude': 'longitude', 'LocationId': 'location', 'GeoInfo':'coordinates', 'DataAqi': 'AQI'
                      }
USELESS_COLS = ['220', 'BatLow', 'RecieveDate', 'GeoInfo', 'Date', 'SendDate', 'latitude', 'longitude',
                'description',
                'FlagBatLowHasFailed', 'FlagPs220HasFailed', 'IsNotSaveData',
                'ParentDeviceId', 'SourceType', 'tags','DataProviderId',
 'IsDeleted', 'IsManualParamLinks', 'IsStartInterval1H', 'ManualPacketParamLinks', 'PacketId','Timestamp']


def unpack_cols(df, *cols_to_unpack):
    for col in cols_to_unpack:
        df = df.assign(**df[col].apply(pd.Series)).drop(col, 1)
    return prep_df(df)

def prep_dicts(dicts, keymap, keys_to_drop, dropna=True):
    """
    :param dicts: list of old dict
    :type dicts: [dicts]
    :param keymap: [{:keys from-keys :values to-keys} keymap]
    :returns: new dict
    :rtype: dict
    """
    res = []
    for d in dicts:
        new_dict = {}
        for key, value in zip(d.keys(), d.values()):
            if key in keys_to_drop:
                continue
            if dropna and not value:
                continue
            if key == 'is_offline':
                new_dict['is_online'] = not value
                continue
            new_key = keymap.get(key, key)
            if 'date' in new_key:
                value = to_date(value)
            new_dict[new_key] = value
        res.append(OrderedDict(sorted(new_dict.items(), key=lambda item: getsizeof(item[1]))))
    return res


def to_date(date_string):
    if isinstance(date_string, datetime.datetime):
        return date_string
    else:
        return pd.to_datetime(date_string, dayfirst=True, utc=True)


def prep_df(df: pd.DataFrame, right_param_names: dict = RIGHT_PARAMS_NAMES, cols_to_drop: List[str] = USELESS_COLS,
            dicts_cols: List[str] = [], dropna=True, index_col: str = None):
    res = df.rename(right_param_names, axis=1)
    res.drop(cols_to_drop, axis=1, inplace=True, errors='ignore')
    for col in dicts_cols:
        res[col] = res[col].apply(prep_dicts, args=[right_param_names, cols_to_drop, dropna])
    if dropna:
        res.dropna(how='all', axis=1, inplace=True)
    for col in res:
        if 'date' in col.lower():
            res[col] = res[col].apply(to_date)
    try:
        res['is_online'] = ~ res['is_offline']
        res.drop('is_offline', axis=1, inplace=True)
    except KeyError:
        pass
    if index_col:
        res.set_index(index_col, inplace=True)
    return res


def timeit(method):
    """
    Decorator to print time elapsed

    """

    @wraps(method)
    def timed(*args, **kwargs):
        timeit = kwargs.get('timeit', False)
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        if timeit:
            print(f"{te - ts:.2f} seconds took to {method.__name__} of size {sys.getsizeof(result) / 1000: .2f} KB.args were{', '.join(map(str, args[1:]))}.kwargs were: {kwargs}")
        return result

    return timed


def debugit(method):
    """
    Decorator to print raw response and request data

    """

    @wraps(method)
    def print_request_response(*args, **kwargs):
        obj = args[0]
        body = {"User": getattr(obj, 'user'), "Pwd": getattr(obj, 'psw'), **kwargs}
        url = f"{getattr(obj, 'host_url', None)}/{args[1]}"
        result = method(*args, **kwargs)
        if kwargs.get('debugit'):
            print(f"url: {url}\nrequest_body: {body}\nresponse: {result}")
        return result

    return print_request_response
