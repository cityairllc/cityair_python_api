import datetime
import json
import logging
import re
from collections import Counter
from collections.abc import Iterable
from enum import Enum
from pprint import pformat
from typing import Dict, Iterator, List, Tuple, Union

import pandas as pd
import requests
from cached_property import cached_property

from .exceptions import (
    CityAirException, EmptyDataException, NoAccessException, ServerException,
    anonymize_request)
from .settings import DEFAULT_HOST, DEVICES_PACKETS_URL, DEVICES_URL, \
    FULL_LOGS_URL, LOGS_URL, LOG_CHECKINFO_ADDITIONAL_FILTER_SUFFIX, \
    LOG_CHECKINFO_FILTER_PATTERN, LOG_EXTRACT_PATTERN, \
    LOG_PACKET_ADDITIONAL_FILTER_SUFFIX, LOG_PACKET_FILTER_PATTERN, \
    PACKET_SENDER_IDS, STATIONS_PACKETS_URL, STATIONS_URL
from .utils import (MAIN_DEVICE_PARAMS, MAIN_STATION_PARAMS, RIGHT_PARAMS_NAMES,
                    USELESS_COLS, add_progress_bar, debugit, get_credentials,
                    parse_checkinfo, prep_df, prep_dicts, timeit, to_date,
                    unpack_cols)


class Period(Enum):
    FIVE_MINS = 1
    TWENTY_MINS = 2
    HOUR = 3
    DAY = 4


# TODO verify - False (in request)
# TODO proper logging, remove printing (pformat request)
# TODO move settings to separate file
# TODO refactor useless_cols mess


class CityAirRequest:
    """
    Object for accessing data of CityAir.io project
    """

    def __init__(self, user: str = None, psw: str = None, **kwargs):
        """
        Parameters
        ----------
        user, psw:  str
            authentication information
        host_url: str, default {DEFAULT_HOST}
            url of the CityAir API, you may want to change it in case using a
            StandAloneServer
        timeout: int, default 100
            timeout for the server request
        verify_ssl: bool, default True
            whether to verify SSL certificate
        silent: bool, default False
            whether
        """
        self.host_url = kwargs.get('host_url', DEFAULT_HOST)
        self.timeout = kwargs.get('timeout', 100)
        self.verify_ssl = kwargs.get('verify_ssl', True)
        if user and psw:
            self.user, self.psw = user, psw
        else:
            self.user, self.psw = get_credentials(kwargs.get("silent", False))

    @cached_property
    def _device_by_serial(self):
        devices_data = self._make_request(DEVICES_URL, "Devices")
        return dict(zip([(data.get('SerialNumber')) for data in devices_data],
                        [data.get('DeviceId') for data in devices_data]))

    @cached_property
    def _device_value_types(self):
        value_types_data = self._make_request(DEVICES_URL,
                                              "PacketsValueTypes")
        return dict(zip([(data.get('ValueType')) for data in value_types_data],
                        [data.get('TypeName') for data in value_types_data]))

    @cached_property
    def _stations_value_types(self):
        value_types_data = self._make_request(STATIONS_URL,
                                              "PacketValueTypes")
        return dict(zip(
            [info['ValueType'] for info in value_types_data],
            [info['TypeName'] for info in value_types_data]
        ))

    @cached_property
    def _device_by_id(self):
        devices_data = self._make_request(DEVICES_URL, "Devices")
        device_by_id = dict(
            zip([(data.get('DeviceId')) for data in devices_data],
                [data.get('SerialNumber') for data in devices_data]))
        for device in devices_data:
            for child in device.get('ChildDevices', []):
                device_by_id.update({child["DeviceId"]: child['SerialNumber']})
        return device_by_id

    @cached_property
    def _device_and_children_by_id(self):
        devices_data = self._make_request(DEVICES_URL, "Devices")
        res = {}
        for data in devices_data:
            key = data.get('DeviceId')
            serials = [data.get('SerialNumber')]
            for child_data in data.get('ChildDevices'):
                serials.append(child_data.get('SerialNumber'))
                res[child_data.get('Id')] = [child_data.get('SerialNumber')]
            res[key] = serials
        return res

    @cached_property
    def _stations_by_device(self):
        stations = self.get_stations(format='dicts')
        res = {}
        for station in stations:
            device_list = station.pop('devices')
            for device in device_list:
                try:
                    if station not in res[device]:
                        res[device].append(station.copy())
                except KeyError:
                    res[device] = [station.copy()]
        return res

    @debugit
    @timeit
    def _make_request(self, method_url: str, *keys: str,
                      silent: bool = True, **kwargs: object):
        """
        Making request to cityair backend

        Parameters
        ----------
        method_url :  str
            url of the specified method
        *keys: [str]
            keys, which data to return from the raw server response
        silent: bool, default True
            whether to raise EmptyDataException if requested data is empty
        **kwargs : dict
            additional args which are directly passed to the request body
        -------"""
        body = {"User": getattr(self, 'user'), "Pwd": getattr(self, 'psw'),
                **kwargs}
        url = f"{self.host_url}/{method_url}"
        response = requests.post(url, json=body, timeout=self.timeout,
                                 verify=self.verify_ssl)
        logging.debug("post request to url: %s\nbody:%s", url, pformat(body))
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise CityAirException(f"Got HTTP error: {e}") from e
        try:
            response_json = response.json()
        except json.JSONDecodeError as e:
            raise CityAirException(
                f"Suddenly got empty json. Couldn't decode it: {e}") from e
        if response_json.get('IsError'):
            raise ServerException(response)
        response_data = response_json.get('Result')
        for key in keys:
            if len(response_data[key]) == 0:
                if not silent:
                    raise EmptyDataException(response=response)
                logging.warning("There are no %s available.\n"
                                "url:%s\n"
                                "filter%s", key, url, anonymize_request(body))
        if len(keys) == 0:
            return response_data
        elif len(keys) == 1:
            return response_data[keys[0]]
        else:
            return [response_data[key] for key in keys]

    def get_devices(self, format: str = 'list', include_offline: bool = True,
                    include_children: bool = False,
                    time=False, debug=False) \
            -> Union[List[str], pd.DataFrame, List[dict]]:
        """
        Provides devices information in various formats

        Parameters
        ----------
        format:  {'list', 'df', 'dicts', 'raw'}, default 'list'
            * 'list' : returns list of serial_numbers
            * 'df' : returns pd.DataFrame pretty formatted
            * 'dicts' : returns list of dictionaries, each including various
            params
            * 'raw' : returns pd.DataFrame including all info got from
            server, other params are ignored
        include_offline: bool, default True
            whether to include offline devices to the output
        include_children : bool, default False
            whether to include info of child devices to the output
        time: bool, default False
            whether to print how long it took to gather data
        debug: bool, default False
            whether to print raw request and response data
        -------"""
        devices_data = self._make_request(DEVICES_URL, "Devices",
                                          time=time, debug=debug)
        df = pd.DataFrame.from_records(devices_data)
        if format == 'raw':
            return df
        df = prep_df(df, dicts_cols=['children'])

        if not include_offline:
            df = df[df['is_online']]
        df_with_children = df.copy()
        for children in df['children']:
            for child in children:
                df_with_children = df_with_children.append(child,
                                                           ignore_index=True)
        if include_children:
            df = df_with_children
        df.set_index('serial_number', inplace=True, drop=False)
        df['stations'] = pd.Series(getattr(self, '_stations_by_device', []))
        df['children'].apply(
            lambda children_info: [child_info.pop('id') for child_info in
                                   children_info] if isinstance(
                children_info,
                Iterable) else [])
        if format == 'dicts':
            res = []
            for serial_number, row in df.iterrows():
                row = dict(row)
                single_dict = {param: row.pop(param, None) for param in
                               MAIN_DEVICE_PARAMS}
                single_dict.update(misc=row)
                res.append(single_dict)
            return res
        elif format == 'list':
            return list(df.index)
        elif format == 'df':
            return df.set_index('serial_number')
        else:
            raise ValueError(
                f"Unknown type of format argument: {format}. Available "
                f"formats are: 'list', 'df', 'dicts', 'raw'")

    @add_progress_bar
    def get_device_data(self, serial_number: str, start_date=None,
                        finish_date=None, last_packet_id=None,
                        take_count: int = 500, all_cols=False,
                        format: str = 'df', verbose=True,
                        time=False, debug=False) \
            -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Provides data from the selected device

        Parameters
        ----------
        serial_number: str
            serial_number of the device
        start_date, finish_date: str or datetime.datetime
            dates on which data is being queried
        last_packet_id: int, default None
            if passed, packets will be queried starting from this packet_id
        take_count: int, default 1000
            count of packets which is requested from the server
        all_cols: bool, default False
            whether to keep or drop columns which are not directly related
            to air
             quality data (i.e. battery status, ps 220, recieve date)
        format:  {'df', 'dict'}, default 'df'
            * 'df' : returns one pd.DataFrame, where value_name is
            concatenated with
                     serial_number of the device if there is more than one
                     device
                     measuring values of a type
            * 'dict' : returns dictionary, where key is serial_number of
                       the device and value is pd.DataFrame containing all
                       data of the device
        verbose: bool, default True:
            whether to show progress bar
        time: bool, default False
            whether to print how long it took to gather data
        debug: bool, default False
            whether to print raw request and response data
        -------"""

        device_id = self._device_by_serial.get(serial_number)
        if not device_id:
            raise NoAccessException(serial_number)
        filter_ = {'Take': take_count,
                   'DeviceId': device_id}
        if last_packet_id:
            filter_['FilterType'] = 2
            filter_['LastPacketId'] = last_packet_id
        elif start_date:
            filter_['FilterType'] = 1
            filter_['TimeBegin'] = to_date(start_date).isoformat()
            filter_['TimeEnd'] = to_date(
                finish_date).isoformat() if finish_date else \
                datetime.datetime.now().isoformat()
        else:
            filter_['FilterType'] = 3
            filter_['Skip'] = 0
        packets = self._make_request(DEVICES_PACKETS_URL, 'Packets',
                                     Filter=filter_, time=time, silent=False,
                                     debug=debug)
        df = pd.DataFrame.from_records(packets)

        df = unpack_cols(df, ['ServiceData'])
        df.drop(['DataJson'], 1, inplace=True, errors='ignore')
        records = []
        for packets in df['Data']:
            #  packets = json.loads(packets)
            records.append(dict(zip(
                [f"value {packet['D']} {packet['VT']}" for packet in
                 packets],
                [packet['V'] for packet in packets])))
        df = df.assign(**pd.DataFrame.from_records(records))
        values_cols = list(
            filter(lambda col: col.startswith('value'), df.columns))
        if format == 'dict':
            res = dict()
            for col in values_cols:
                _, device_id, value_id = col.split(' ')
                serial = self._device_by_id[int(device_id)]
                value_name = self._device_value_types[int(value_id)]
                series_to_append = df[col].rename(value_name)
                try:
                    res[serial] = pd.concat([res[serial], series_to_append],
                                            axis=1)
                except KeyError:
                    res[serial] = pd.concat([df['date'], series_to_append],
                                            axis=1)
            try:
                res[serial_number] = pd.concat(
                    [df.drop(values_cols + ['Data', 'SendDate', 'date'],
                             axis=1, errors='ignore'), res[serial_number]],
                    axis=1)
            except KeyError:
                res[serial_number] = df.drop(values_cols + ['Data'], axis=1,
                                             errors='ignore')
            for device in res:
                res[device] = prep_df(res[device], index_col='date',
                                      cols_to_unpack=['coordinates'],
                                      cols_to_drop=[] if all_cols else
                                      USELESS_COLS)
            return res
        elif format == 'df':
            value_types_count = Counter(list(
                map(lambda s: (s.split(' ')[-1]), values_cols)))
            for col in list(
                    filter(lambda col: col.startswith('value'), df.columns)):
                _, device_id, value_id = col.split(' ')
                serial = self._device_by_id[int(device_id)]
                value_name = self._device_value_types[int(value_id)]
                if value_types_count[value_id] > 1:
                    proper_col_name = f"{value_name} [{serial}]"
                else:
                    proper_col_name = f"{value_name}"
                df.rename(columns={col: proper_col_name}, inplace=True)
            df = prep_df(df.drop(['Data'], axis=1),
                         right_param_names=RIGHT_PARAMS_NAMES,
                         index_col='date', cols_to_unpack=['coordinates'],
                         cols_to_drop=[] if all_cols else USELESS_COLS)
            return df
        else:
            raise ValueError(
                f"Unknown option of format argument: {format}. Available "
                f"formats are: 'df', 'dict'")

    def get_stations(self, format: str = 'list', include_offline: bool = True,
                     time=False, debug=False) \
            -> Union[List[str], pd.DataFrame, List[dict]]:
        """
        Provides devices information in various formats

        Parameters
        ----------
        format:  {'list', 'df', 'dicts', 'raw'}, default 'list'
            * 'list' : returns list of serial_numbers
            * 'df' : returns pd.DataFrame pretty formatted
            * 'dicts' : returns list of dictionaries, each including various
            params
            * 'raw' : returns pd.DataFrame including all info got from
            server, other params are ignored
        include_offline: bool, default True
           whether to include offline devices to the output
        time: bool, default False
           whether to print how long it took to gather data
        debug: bool, default False
           whether to print raw request and response data
        -------"""
        locations_data, stations_data, devices_data = self._make_request(
            STATIONS_URL, "Locations",
            "MoItems", "Devices",
            time=time, debug=debug)
        locations = dict(
            zip([(data.get('LocationId')) for data in locations_data],
                [data.get('Name') for data in locations_data]))
        for device_data in devices_data:
            self._device_by_id.update({device_data.get(
                'DeviceId'): device_data.get('SerialNumber')})
        df = pd.DataFrame.from_records(stations_data)
        if format == 'raw':
            return df
        df = prep_df(df, index_col='id', dropna=False,
                     cols_to_unpack=['coordinates'])
        df['devices'] = df['devices_auto'].apply(
            lambda link: self._device_and_children_by_id.get(
                link.get('DeviceId')) if link else [])
        df['devices'] += df['devices_manual'].apply(
            lambda links: [self._device_by_id.get(link.get('DeviceId')) for
                           link in
                           links] if links else [])
        df['devices'] = df['devices'].apply(
            lambda x: x if isinstance(x, list) else [])
        df.drop(['devices_auto', 'devices_manual'], axis=1, inplace=True)
        df['location'] = df['location'].apply(
            lambda id_: locations.get(id_, None) if id_ else None)

        if not include_offline:
            df = df[df['is_online']]
        if format == 'dicts':
            res = []
            for _, row in df.reset_index().iterrows():
                res.append({param: row.get(param) for param in
                            MAIN_STATION_PARAMS})
            return res
        elif format == 'list':
            return list(df.index)
        elif format == 'df':
            return df
        else:
            raise ValueError(
                f"Unknown type of format argument: {format}. Available "
                f"formats are: 'list', 'df', 'dicts', 'raw'")

    @add_progress_bar
    def get_station_data(self, station_id: int, start_date=None,
                         finish_date=datetime.datetime.now(),
                         take_count: int = 1000,
                         period: Period = Period.TWENTY_MINS, verbose=True,
                         time=False, debug=False) -> pd.DataFrame:
        """
        Provides data from the selected station
        Parameters
        ----------
        station_id : int
            id of the station
        start_date, finish_date: str or datetime.datetime
            dates on which data is being queried
        take_count : int, default 1000
            count of packets which is requested from the server
        period: Period (enum), default cityair_api.Period.TWENTY_MINS
            period could be five mins, twenty mins, hour, day
        verbose: bool, default True:
            whether to show progress bar
        time: bool, default False
            whether to print how long it took to gather data
        debug: bool, default False
            whether to print raw request and response data
        -------"""
        filter_ = {'TakeCount': take_count,
                   'MoId': station_id,
                   'IntervalType': period.value}
        if start_date:
            filter_['FilterType'] = 1
            filter_['BeginTime'] = to_date(start_date).isoformat()
            filter_['EndTime'] = to_date(
                finish_date).isoformat() if finish_date else \
                datetime.datetime.now().isoformat()
        else:
            filter_['FilterType'] = 3
            filter_['SkipFromLast'] = 0
        packets = self._make_request(STATIONS_PACKETS_URL, 'Packets',
                                     Filter=filter_, time=time, debug=debug,
                                     silent=False)
        df = pd.DataFrame.from_records(packets)
        records = []
        for packets in df['DataJson']:
            packets = json.loads(packets)
            records.append(dict(zip(
                [self._stations_value_types.get(packet['Id'], 'undefined')
                 for packet in packets],
                [packet['Sum'] / packet['Cnt'] for packet in packets])))
        df = df.assign(**pd.DataFrame.from_records(records))
        df = prep_df(df.drop(['DataJson'], axis=1), index_col='date')
        return df

    def get_locations(self) -> List[dict]:
        """
        Provides information on locations including stations and devices
        """
        stations = self.get_stations(format='dicts')
        stations_by_location = {}
        for station in stations:
            location = station.pop('location')
            if location:
                try:
                    stations_by_location[location].append(station)
                except KeyError:
                    stations_by_location[location] = [station]

        locations_data = self._make_request(STATIONS_URL, "Locations")
        locations_data = prep_dicts(locations_data, RIGHT_PARAMS_NAMES,
                                    USELESS_COLS + ['LocationId'])
        for location_data in locations_data:
            location_data['stations'] = stations_by_location.get(
                location_data.get('name'))
        return locations_data

    def _expand_log_item(self, log_item: dict, extract_re) -> dict:
        """
        if log items has longer version it's making request and
        retrieves full version
        adds "msg" key to the log_item
        also extracts packet and adds "packet" key to the log_item
        """
        if log_item['IsMessageExt']:
            full_log_items = self._make_request(FULL_LOGS_URL,
                                                "LogsItems",
                                                LogIds=[log_item["LogId"]])
            log_item.update(msg=full_log_items[0]['Message'])
        else:
            log_item.update(msg=log_item['MessageShort'])
        log_item.update(packet=extract_re.search(log_item['msg']).group(0))
        return log_item

    def get_logs(self, serial_number: str, type: str = "packet",
                 start_date=None, finish_date=None, take_count=1000,
                 app_sender_ids=PACKET_SENDER_IDS, to_include_date=False,
                 extract_pattern=LOG_EXTRACT_PATTERN, filter_pattern="") \
            -> Iterator[Union[str, Tuple[datetime.datetime, str]]]:
        f"""
        retrieves log records and extracts records of specific types
        Parameters
        ----------
        serial_number: str
            serial of the device, also could be any string for backend to filter
            log messages
        type: str {'packet', 'checkinfo', 'custom'}, default 'packet'
            * 'packet' - searching log messages for raw_packets sent by device
            * 'checkinfo' - searching for check info packets
            * 'custom' - to auto filtering is performed, extract_pattern and 
              search_pattern args should be provided to perform filtering
        start_date: datetime or str, default None
        finish_date: datetime or str, default None
        take_count: int, default 1000
            packets count are fetched before filtering
        app_sender_ids: dict, default {PACKET_SENDER_IDS}
            passed to backend for filtering, 
        to_include_date: bool, default False
            if to include date to the return           
        extract_pattern: str
            regex for extraction of packet from log message
        filter_pattern: str, {LOG_PACKET_FILTER_PATTERN} if type='packet'
                             {LOG_CHECKINFO_FILTER_PATTERN} if type='checkinfo'
            regex for filtering log records
        Returns
        -------
            list of log_messages or tuples(datetime, log_message)

        """
        if type == 'packet':
            filter_pattern = LOG_PACKET_FILTER_PATTERN
            serial_number += LOG_PACKET_ADDITIONAL_FILTER_SUFFIX
        elif type == 'checkinfo':
            filter_pattern = LOG_CHECKINFO_FILTER_PATTERN
            serial_number += LOG_CHECKINFO_ADDITIONAL_FILTER_SUFFIX
        elif type == 'custom':
            pass
        else:
            raise ValueError("type arg should one of the ('packet', "
                             "'checkinfo', 'custom')")
        filter_re = re.compile(filter_pattern)
        extract_re = re.compile(extract_pattern)

        filter_ = {'AppSenderIds': app_sender_ids,
                   'BeginDate': to_date(start_date),
                   'EndDate': to_date(finish_date),
                   'EventTypeIds': [],
                   'MaxLogItemsCount': take_count,
                   'MessageFilterString': serial_number}
        log_items = self._make_request(LOGS_URL, "LogsItems", Filter=filter_)
        filtered_log_items = filter(
            lambda item: filter_re.search(item['MessageShort']),
            log_items)
        full_items = map(lambda item: self._expand_log_item(item, extract_re),
                         filtered_log_items)

        for log_item in full_items:
            if to_include_date:
                yield to_date(log_item['Time']), log_item['packet']
            else:
                yield log_item['packet']

    def get_last_log(self, serial_number: str, type: str = "packet") -> str:
        f"""
        retrieves log records and extracts records of specific types
        Parameters
        ----------
        serial_number: str
        type: str {'packet', 'checkinfo'}, default 'packet'
            * 'packet' - searching log messages for raw_packets sent by device
            * 'checkinfo' - searching for check info packets
        Returns
        -------
            last log message

        """
        if type not in ('packet', 'checkinfo'):
            raise ValueError("type arg shoud be either 'packet' or 'checkinfo'")
        msgs = self.get_logs(serial_number, type=type, take_count=1)
        return next(msgs)

    def get_last_rawpacket(self, serial_number: str) -> str:
        f"""
        retrieves filtered log records and extracts packet sent by device
        Parameters
        ----------
        serial_number: str
        Returns
        -------
            last raw packet

        """
        msgs = self.get_logs(serial_number, type="packet", take_count=1)
        return next(msgs)

    def get_last_checkinfo(self, serial_number: str) -> List[dict]:
        f"""
        retrieves filtered log records and extracts checkinfo sent by device
        Parameters
        ----------
        serial_number: str
        Returns
        -------
            last checkinfo

        """
        msg = next(self.get_logs(serial_number, type="checkinfo", take_count=1))
        return parse_checkinfo(msg)


CAR = CityAirRequest
