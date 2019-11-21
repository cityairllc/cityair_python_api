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
RIGHT_PARAMS_NAMES = {'FlagPs220': '220', 'RecvDate': 'receive_date', 'Ps220': '220','GsmRssi': 'rssi',
                      'SendDate': 'date', 'Temperature': 'T', 'BatLow': 'is_bat_low',
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
                      'Latitude': 'latitude', 'Longitude': 'longitude', 'LocationId': 'location',
                      'GeoInfo': 'coordinates','Geo': 'coordinates', 'DataAqi': 'AQI', 'GmtHour':'gmt_hour_diff', 'PublishOnMap': 'is_public', 'NameRu': 'name_ru'
                      }
USELESS_COLS = ['220', 'BatLow', 'receive_date', 'GeoInfo','Geo', 'Date', 'SendDate', 'latitude', 'longitude',
                'description', 'coordinates','rssi',
                'FlagBatLowHasFailed', 'FlagPs220HasFailed', 'IsNotSaveData',
                'ParentDeviceId', 'SourceType', 'tags', 'DataProviderId',
                'IsDeleted', 'IsManualParamLinks', 'IsStartInterval1H', 'ManualPacketParamLinks', 'PacketId',
                'Timestamp', 'is_bat_low', 'BounceNorth','BounceSouth', 'BounceEast','BounceWest', 'CountryId',

                'BounceNorth', 'BounceSouth', 'BounceEast', 'BounceWest',
                'GmtHour', 'LocationUrl']


def unpack_cols(df, cols_to_unpack, right_params_names=RIGHT_PARAMS_NAMES):
    for col in cols_to_unpack:
        df = df.assign(**df[col].apply(pd.Series)).drop(col, 1)
    df.rename(right_params_names, axis=1, inplace=True)
    return df


def prep_dicts(dicts, newkeys, keys_to_drop, dropna=True):
    res = []
    for d in dicts:
        new_dict = {}
        for key, value in zip(d.keys(), d.values()):
            if key in keys_to_drop:
                continue
            if dropna and value == None:
                continue
            if key == 'is_offline':
                new_dict['is_online'] = not value
                continue
            new_key = newkeys.get(key, key)
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
            dicts_cols: List[str] = [], dropna: bool = True, index_col: str = None, cols_to_unpack: List[str] = []):

    res = df.copy()
    res.rename(right_param_names, axis=1, inplace=True)

    for col in dicts_cols:
        res[col] = res[col].apply(prep_dicts, args=[right_param_names, cols_to_drop, dropna])
    try:
        res = unpack_cols(res, cols_to_unpack)
    except KeyError as e:
        pass
    if dropna:
        res.dropna(how='all', axis=1, inplace=True)
    for col in res:
        if 'date' in col.lower():
            res[col] = res[col].apply(to_date)
    try:
        res['is_online'] = ~ res['is_offline']
        res.drop('is_offline', axis=1, inplace=True)
    except KeyError as e:
        pass
    if index_col and index_col in df.columns:
        res.set_index(index_col, inplace=True)
    res.rename(right_param_names, axis=1, inplace=True)
    res = res.drop(cols_to_drop, axis=1, errors='ignore')
    return res


def timeit(method):
    """
    Decorator to print time elapsed

    """

    @wraps(method)
    def timed(*args, **kwargs):
        timeit = kwargs.pop('timeit', False)
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        if timeit:
            print(f"{te - ts:.2f} seconds took to {method.__name__} of size"
            f"{sys.getsizeof(result) / 1000: .2f} KB\nargs were"
                  f" {', '.join(map(str, args[1:]))}\nkwargs were: {kwargs}")
        return result

    return timed


def debugit(method):
    """
    Decorator to print raw response and request data

    """

    @wraps(method)
    def print_request_response(*args, **kwargs):
        debugit_ = kwargs.pop('debugit', False)
        obj = args[0]
        body = {"User": getattr(obj, 'user'), "Pwd": getattr(obj, 'psw'), **kwargs}
        url = f"{getattr(obj, 'host_url', None)}/{args[1]}"
        result = method(*args, **kwargs)
        if debugit_:
            print(f"url: {url}\nrequest_body: {body}\nresponse: {result}")
        return result

    return print_request_response
