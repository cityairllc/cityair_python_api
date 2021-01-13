import datetime
import logging
import re
import sys
import time
from collections import defaultdict
from functools import wraps
from typing import List, Optional, Union

import pandas as pd
import progressbar
import pytz

from cityair_api.settings import CHECKINFO_PARSE_PATTERN
from .exceptions import EmptyDataException

logger = logging.getLogger(__name__)

FATHER_PREFIXES = ["CA", "ROOFTOP", "LAB"]


def is_main_device(serial):
    for prefix in FATHER_PREFIXES:
        if serial.startswith(prefix):
            return True
    return False


RIGHT_PARAMS_NAMES = {
        'FlagPs220': '220',
        'RecvDate': 'receive_date', 'Ps220': '220',
        'GsmRssi': 'rssi',
        'SendDate': 'date', 'Temperature': 'T',
        'BatLow': 'is_bat_low',
        'Humidity': 'RH', 'Pressure': 'P',
        'ChildDevices': 'children',
        'DataDeliveryPeriodSec': 'delivery_period',
        'Description': 'description',
        'DeviceCheckInfos': 'check_infos',
        'DeviceFirstDate': 'first_packet_date',
        'DeviceIMEI': 'IMEI',
        'DeviceIMSI': 'IMSI',
        'DeviceLastDate': 'last_packet_date',
        'DeviceLastGeo': 'coordinates',
        'DeviceName': 'name',
        'FlagBatLow': 'is_bat_low', 'DeviceId': 'id',
        'IsOffline': 'is_offline',
        'SerialNumber': 'serial_number',
        'SoftwareVersion': 'software',
        'SourceType': 'type', 'TagIds': 'tags',
        'MoId': 'id',
        'Name': 'name',
        'PublishName': 'internal_name',
        'PublishNameRu': 'name_ru',
        'ManualDeviceLinks': 'devices_manual',
        'DeviceLink': 'devices_auto',
        'GmtOffset': 'gmt_offset',
        'DotItem': 'coordinates',
        'Latitude': 'latitude',
        'Longitude': 'longitude',
        'LocationId': 'location',
        'GeoInfo': 'coordinates',
        'Geo': 'coordinates', 'DataAqi': 'AQI',
        'GmtHour': 'gmt_hour_diff',
        'PublishOnMap': 'is_public',
        'NameRu': 'name_ru',
        'PacketId': 'packet_id',
        "Humidity_": "RH"
        }
USELESS_COLS = ['220', 'BatLow', 'receive_date', 'GeoInfo', 'Geo', 'Date',
                'SendDate', 'ResetMoData', 'description', 'coordinates',
                'rssi', 'FlagBatLowHasFailed', 'FlagPs220HasFailed',
                'IsNotSaveData', 'ParentDeviceId', 'SourceType', 'tags',
                'DataProviderId', 'IsDeleted', 'IsManualParamLinks',
                'IsStartInterval1H', 'ManualPacketParamLinks', 'Timestamp',
                'is_bat_low', 'BounceNorth', 'BounceSouth', 'BounceEast',
                'BounceWest', 'CountryId', 'BounceNorth', 'BounceSouth',
                'BounceEast', 'BounceWest', 'GmtHour', 'LocationUrl',
                'DistributionSummary', 'SortRank', 'PacketId', 'packet_id',
                "DoS", "DDW", 'Pcf', 'Scf']

MAIN_DEVICE_PARAMS = ['serial_number', 'name', 'software', 'stations',
                      'children', 'check_infos']
MAIN_STATION_PARAMS = ['id', 'name', 'name_ru', 'location', 'gmt_offset',
                       'devices', 'latitude', 'longitude']


def add_progress_bar(method):
    """
    Decorator to display progress bar

    """
    progress_scaler = 10 ** 6
    default_take_count = 500

    @wraps(method)
    def progressed(*args, **kwargs):
        start_date = to_date(kwargs.get('start_date'))
        if not start_date:
            return method(*args, **kwargs)
        finish_date = to_date(
                kwargs.get('finish_date', datetime.datetime.utcnow()))
        kwargs.update(take_count=kwargs.get('take_count', default_take_count))
        bar = progressbar.ProgressBar(max_value=(finish_date - start_date)
                                      .total_seconds() / progress_scaler,
                                      widgets=[
                                              f"{args[1]}",
                                              ': ',
                                              progressbar.Percentage(),
                                              '    ',
                                              progressbar.Timer(),
                                              progressbar.Bar(),
                                              progressbar.ETA()
                                              ],
                                      max_error=False
                                      )
        if kwargs.get('format', 'df') == 'df':
            res = pd.DataFrame()
        else:
            res = defaultdict(pd.DataFrame)
        while finish_date - start_date > datetime.timedelta(hours=1):
            try:
                data = method(*args, **kwargs)
                if isinstance(res, pd.DataFrame):
                    res = pd.concat([res, data], sort=False)
                    start_date = res.index[-1]
                else:
                    for serial, df in data.items():
                        res[serial] = pd.concat([res[serial], df], sort=False)
                    start_date = max([res[key].index[-1] for key in res])
            except EmptyDataException:
                start_date += datetime.timedelta(days=2)
            kwargs.update(
                    start_date=start_date + datetime.timedelta(seconds=30))
            fetched_seconds = (finish_date.replace(tzinfo=pytz.utc)
                               - start_date.replace(
                            tzinfo=pytz.utc)).total_seconds()
            bar.update(bar.max_value - (fetched_seconds / progress_scaler))
        size = len(res) if isinstance(res, pd.DataFrame) else max(
                map(len, res.values()))
        logger.info(f'finished acquiring {args[1]} data of size {size}')
        if size == 0:
            raise EmptyDataException(request_args=kwargs)
        return res

    return progressed


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
            if dropna and value is None:
                continue
            if key == 'is_offline':
                new_dict['is_online'] = not value
                continue
            new_key = newkeys.get(key, key)
            if 'date' in new_key:
                value = to_date(value)
            new_dict[new_key] = value
        res.append(new_dict)
    return res


def parse_checkinfo(msg: str) -> List[dict]:
    parsed_checkinfo = []
    parse_re = re.compile(CHECKINFO_PARSE_PATTERN)
    infos = parse_re.findall(msg.replace(", ", ","))
    if not infos:
        raise ValueError("checkinfo data should be in the format"
                         f" of \"{CHECKINFO_PARSE_PATTERN}\"")
    for info in infos:
        module, status, count, details = info.split(',')
        parsed_checkinfo.append(dict(
                module=module,
                has_error=False if status == 'ok' else True,
                errors_count=int(count),
                details=str(details).replace("\"", "")))
    return parsed_checkinfo


def to_date(date: Union[datetime.datetime, str, None],
            format: str = 'date') -> Optional[Union[str, datetime.datetime]]:
    """

    :param date: date (datetime or string) to be converted
    :param format: str, 'str' or 'date', default 'date'
        return type
    :return:
    """
    if not date:
        return None
    if isinstance(date, datetime.datetime):
        pass
    elif isinstance(date, str):
        date = pd.to_datetime(date, dayfirst=True)
    else:
        raise ValueError(f"date should be 'str' or 'datetime', "
                         f"got {type(date)} instead")

    if format == 'str':
        return date.isoformat()
    elif format == 'date':
        return date.replace(tzinfo=None)
    else:
        raise ValueError(f"format arg should be one of the 'str', 'date', "
                         f"got {format} instead")


def prep_df(df: pd.DataFrame, right_param_names: dict = RIGHT_PARAMS_NAMES,
            cols_to_drop: List[str] = USELESS_COLS,
            dicts_cols: List[str] = [], dropna: bool = True,
            index_col: str = None, cols_to_unpack: List[str] = []):
    res = df.copy()
    res.dropna(how='all', axis=0)
    res.rename(right_param_names, axis=1, inplace=True)

    for col in dicts_cols:
        res[col] = res[col].apply(prep_dicts,
                                  args=[right_param_names, cols_to_drop,
                                        dropna])
    try:
        res = unpack_cols(res, cols_to_unpack)
    except (KeyError, TypeError):
        pass
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
    if index_col and index_col in res.columns:
        res.set_index(index_col, inplace=True)
    res.rename(right_param_names, axis=1, inplace=True)
    res = res.drop(cols_to_drop, axis=1, errors='ignore')
    res = res.sort_index()
    return res


def timeit(method):
    """
    Decorator to print time elapsed

    """

    @wraps(method)
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()

        logger.debug(
                f"{te - ts:.2f} seconds took to {method.__name__} of size"
                f"{sys.getsizeof(result) / 1000: .2f} KB\nargs were"
                f" {', '.join(map(str, args[1:]))}\nkwargs were: {kwargs}")
        return result

    return timed
