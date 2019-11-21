import pandas as pd
import datetime
from collections import Counter
import requests
import json
from sys import getsizeof
from collections import OrderedDict
from enum import Enum
from utils import to_date, timeit, unpack_cols, debugit, prep_dicts, prep_df, USELESS_COLS, RIGHT_PARAMS_NAMES
from exceptions import EmptyDataException, NoAccessException, ServerException, CityAirException
from collections.abc import Iterable


class Period(Enum):
    FIVE_MINS = 1
    TWENTY_MINS = 2
    HOUR = 3
    DAY = 4


DEFAULT_HOST = "https://cityair.io/backend-api/request-v2.php?map="


class CityAirRequest:
    f"""
    Object for accessing data of CityAir.io project

    Parameters
    ----------
    user, psw:  str
        authentication information
    host_url: str, default {DEFAULT_HOST}
        url of the CityAir API, you may want to change it in case using a StandAloneServer
    timeout: int, default 100
        timeout for the server request
    -------"""

    def __init__(self, user: str, psw: str, **kwargs):
        self.host_url = kwargs.get('host_url', DEFAULT_HOST)
        self.timeout = kwargs.get('timeout', 100)
        self.user = user
        self.psw = psw
        self.stations_by_device = {}
        self._prepare()

    def _prepare(self):
        value_types_data, devices_data = self._make_request(f"DevicesApi2/GetDevices", "PacketsValueTypes", "Devices")
        self.value_types = dict(zip([(data.get('ValueType')) for data in value_types_data],
                                    [data.get('TypeName') for data in value_types_data]))
        self.device_by_serial = dict(zip([(data.get('SerialNumber')) for data in devices_data],
                                         [data.get('DeviceId') for data in devices_data]))
        self.device_by_id = dict(zip([(data.get('DeviceId')) for data in devices_data],
                                     [data.get('SerialNumber') for data in devices_data]))
        for device in devices_data:
            for child in device.get('ChildDevices', []):
                self.device_by_id.update({child["DeviceId"]: child['SerialNumber']})
        stations = self.get_stations(format='dicts')
        self.stations_by_device = {}
        for station in stations:
            device_list = station.pop('devices')
            for device in device_list:
                try:
                    if station not in self.stations_by_device[device]:
                        self.stations_by_device[device].append(station)
                except KeyError:
                    self.stations_by_device[device] = [station]

    @debugit
    @timeit
    def _make_request(self, method_url, *keys, **kwargs):
        """
        Making request with the prepared data

        Parameters
        ----------
        method_url :  str
            url of the specified method
        *keys: [str]
            keys, which data to return from the raw server response
        **kwargs : dict
            some additional args to pass to the request
        -------"""
        body = {"User": getattr(self, 'user'), "Pwd": getattr(self, 'psw'), **kwargs}
        url = f"{self.host_url}/{method_url}"
        response = requests.post(url, json=body, timeout=self.timeout)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise CityAirException(f"Got HTTP error: {e}") from e
        try:
            response_json = response.json()
        except json.JSONDecodeError as e:
            raise CityAirException(f"Suddenly got empty json. Couldn't decode it: {e}") from e
        if response_json.get('IsError'):
            raise ServerException(response)
        response_data = response_json.get('Result')
        for key in keys:
            if len(response_data[key]) == 0:
                raise EmptyDataException(response=response)
        if len(keys) == 0:
            return response_data
        elif len(keys) == 1:
            return response_data[keys[0]]
        else:
            return [response_data[key] for key in keys]

    def get_devices(self, format: str = 'list', include_offline: bool = True, include_children: bool = False,
                    timeit=False, debugit=False):
        """
        Provides devices information in various formats

        Parameters
        ----------
        format:  {'list', 'df', 'dicts', 'raw'}, default 'list'
            * 'list' : returns list of serial_numbers
            * 'df' : returns pd.DataFrame pretty formatted
            * 'dicts' : returns list of dictionaries, each including various params
            * 'raw' : returns pd.DataFrame including all info got from server, other params are ignored
        include_offline: bool, default True
            whether to include offline devices to the output
        include_children : bool, default False
            whether to include info of child devices to the output
        timeit: bool, default False
            whether to print how long it took to gather data
        debugit: bool, default False
            whether to print raw request and response data
        -------"""
        devices_data = self._make_request(f"DevicesApi2/GetDevices", "Devices",
                                          timeit=timeit, debugit=debugit)
        df = pd.DataFrame.from_records(devices_data)
        if format == 'raw':
            return df
        df = prep_df(df, dicts_cols=['children'])

        if not include_offline:
            df = df[df['is_online']]
        df_with_children = df.copy()
        for children in df['children']:
            for child in children:
                df_with_children = df_with_children.append(child, ignore_index=True)
        if include_children:
            df = df_with_children
        df.set_index('serial_number', inplace=True, drop=False)
        df['stations'] = pd.Series(getattr(self, 'stations_by_device', None))
        df['children'].apply(
            lambda children_info: [child_info.pop('id') for child_info in children_info] if isinstance(children_info,
                                                                                                       Iterable) else [])
        if format == 'dicts':
            res = []
            for serial_number, row in df.iterrows():
                row = dict(row)
                main_params = ['serial_number', 'name', 'children', 'check_infos']
                single_dict = dict(zip(main_params, [row.pop(param, None) for param in main_params]))
                single_dict['misc'] = OrderedDict(sorted(row.items(), key=lambda item: getsizeof(item[1])))
                single_dict.update({'stations': self.stations_by_device.get(serial_number)})
                res.append(OrderedDict(sorted(single_dict.items(), key=lambda item: getsizeof(item[1]))))
            return res
        elif format == 'list':
            return list(df.index)
        elif format == 'df':
            return df
        else:
            raise ValueError(
                f"Unknown type of format argument: {format}. Available formats are: 'list', 'df', 'dicts', 'raw'")

    def get_device_data(self, serial_number: str, start_date=None,
                        finish_date=datetime.datetime.now(),
                        take_count: int = 1000, all_cols=False,
                        separate_device_data: bool = False,
                        timeit=False, debugit=False):
        """
        Provides data from the selected device

        Parameters
        ----------
        serial_number: str
            serial_number of the device
        start_date, finish_date: str or datetime.datetime
            dates on which data is being queried
        take_count: int, default 1000
            count of packets which is requested from the server
        all_cols: bool, default False
            whether to keep or drop columns which are not directly related to air
             quality data (i.e. battery status, ps 220, recieve date)
        separate_device_data: bool, default False
            whether to separate dfs for individual devices.
            if False - returns one pd.DataFrame, where value_name is concatenated with
                serial_number of the device if there is more than one device
                measuring values of a type
            if True - returns dictionary, where keys are serial_number of
                the device and value is pd.DataFrame containing all data of each device
        timeit: bool, default False
            whether to print how long it took to gather data
        debugit: bool, default False
            whether to print raw request and response data
        -------"""
        try:
            device_id = self.device_by_serial[serial_number]
        except AttributeError:
            self._prepare()
            device_id = self.device_by_serial[serial_number]
        except KeyError:
            raise NoAccessException(serial_number)
        filter_ = {'Take': take_count,
                   'DeviceId': device_id}
        if start_date:
            filter_['FilterType'] = 1
            filter_['TimeBegin'] = to_date(start_date).isoformat()
            filter_['TimeEnd'] = to_date(finish_date).isoformat()
        else:
            filter_['FilterType'] = 3
            filter_['Skip'] = 0
        packets = self._make_request("DevicesApi2/GetPackets", 'Packets', Filter=filter_, timeit=timeit,
                                     debugit=debugit)
        df = pd.DataFrame.from_records(packets)

        df = unpack_cols(df, ['ServiceData'])
        df.drop(['DataJson', 'PacketId'], 1, inplace=True, errors='ignore')
        records = []
        for packets in df['Data']:
            #  packets = json.loads(packets)
            records.append(dict(zip(
                [f"value {packet['D']} {packet['VT']}" for packet in packets],
                [packet['V'] for packet in packets])))
        df = df.assign(**pd.DataFrame.from_records(records))
        values_cols = list(filter(lambda col: col.startswith('value'), df.columns))
        if separate_device_data:
            res = dict()
            for col in values_cols:
                _, device_id, value_id = col.split(' ')
                serial = self.device_by_id[int(device_id)]
                value_name = self.value_types[int(value_id)]
                series_to_append = df[col].rename(value_name)
                try:
                    res[serial] = pd.concat([res[serial], series_to_append], axis=1)
                except KeyError:
                    res[serial] = pd.concat([df['date'], series_to_append], axis=1)
                    # res[serial] = series_to_append.to_frame()
            try:
                res[serial_number] = pd.concat(
                    [df.drop(values_cols + ['Data', 'SendDate','date'], axis=1, errors='ignore'), res[serial_number]], axis=1)
            except KeyError:
                res[serial_number] = df.drop(values_cols + ['Date'], axis=1)
            for device in res:
                res[device] = prep_df(res[device], index_col='date', cols_to_unpack=['coordinates'],
                                      cols_to_drop=[] if all_cols else USELESS_COLS)
            return res
        else:
            value_types_count = Counter(list(
                map(lambda s: (s.split(' ')[-1]), values_cols)))
            for col in list(filter(lambda col: col.startswith('value'), df.columns)):
                _, device_id, value_id = col.split(' ')
                serial = self.device_by_id[int(device_id)]
                value_name = self.value_types[int(value_id)]
                if value_types_count[value_id] > 1:
                    proper_col_name = f"{value_name} [{serial}]"
                else:
                    proper_col_name = f"{value_name}"
                df.rename(columns={col: proper_col_name}, inplace=True)
            df = prep_df(df.drop(['Data'], axis=1), right_param_names=RIGHT_PARAMS_NAMES,
                         index_col='date', cols_to_unpack=['coordinates'],
                         cols_to_drop=[] if all_cols else USELESS_COLS)
            return df

    def get_stations(self, format: str = 'list', include_offline: bool = True,
                     timeit=False, debugit=False):
        """
        Provides devices information in various formats

        Parameters
        ----------
        format:  {'list', 'df', 'dicts', 'raw'}, default 'list'
            * 'list' : returns list of serial_numbers
            * 'df' : returns pd.DataFrame pretty formatted
            * 'dicts' : returns list of dictionaries, each including various params
            * 'raw' : returns pd.DataFrame including all info got from server, other params are ignored
        include_offline: bool, default True
           whether to include offline devices to the output
        timeit: bool, default False
           whether to print how long it took to gather data
        debugit: bool, default False
           whether to print raw request and response data
        -------"""
        locations_data, stations_data, devices_data = self._make_request(f"MoApi2/GetMoItems", "Locations",
                                                                         "MoItems", "Devices",
                                                                         timeit=timeit, debugit=debugit)
        locations = dict(zip([(data.get('LocationId')) for data in locations_data],
                             [data.get('Name') for data in locations_data]))
        for device_data in devices_data:
            self.device_by_id.update({device_data.get('DeviceId'): device_data.get('SerialNumber')})
        df = pd.DataFrame.from_records(stations_data)
        if format == 'raw':
            return df
        df = prep_df(df, index_col='id')

        df['devices'] = df['devices_auto'].apply(
            lambda link: self.device_by_id.get(link.get('DeviceId')) if link else None)
        devices_with_children = self.get_devices(format='dicts')
        children = dict(zip([device_info.get('serial_number') for device_info in devices_with_children],
                            [[c.get('serial_number') for c in device_info.get('children')] for device_info in
                             devices_with_children]))

        df['devices'] = df['devices'].apply(lambda serial: [serial] + children.get(serial, []) if serial else [])
        df['devices'] += df['devices_manual'].apply(
            lambda links: [self.device_by_id.get(link.get('DeviceId')) for link in links] if links else [])
        df.drop(['devices_auto', 'devices_manual'], axis=1, inplace=True)
        df['location'] = df['location'].apply(lambda id_: locations.get(id_, None) if id_ else None)

        if not include_offline:
            df = df[df['is_online']]
        if format == 'dicts':
            res = []
            for id_, row in df.iterrows():
                row = dict(row)
                res.append(OrderedDict(
                    [(df.index.name, id_),
                     ('name', row.get('publish_name') or row.get('name')),
                     ('name_ru', row.get('publish_name_ru'))]
                    +
                    [(param, row.get(param)) for param in ('location', 'gmt_offset', 'devices',)]
                ))
            return res
        elif format == 'list':
            return list(df.index)
        elif format == 'df':
            return df
        else:
            raise ValueError(
                f"Unknown type of format argument: {format}. Available formats are: 'list', 'df', 'dicts', 'raw'")

    def get_station_data(self, station_id: int, start_date=None,
                         finish_date=datetime.datetime.now(),
                         take_count: int = 1000, period: Period = Period.TWENTY_MINS,
                         timeit=False, debugit=False):
        """
        Provides data from the selected station
        Parameters
        ----------
        station_id : id
            id of the station
        start_date, finish_date: str or datetime.datetime
            dates on which data is being queried
        take_count : int, default 1000
            count of packets which is requested from the server
        period: Period (enum), default cityair_api.Period.TWENTY_MINS
            period could be five mins, twenty mins, hour, day
        timeit: bool, default False
            whether to print how long it took to gather data
        debugit: bool, default False
            whether to print raw request and response data
        -------"""
        filter_ = {'TakeCount': take_count,
                   'MoId': station_id,
                   'IntervalType': period.value}
        if start_date:
            filter_['FilterType'] = 1
            filter_['BeginTime'] = to_date(start_date).isoformat()
            filter_['EndTime'] = to_date(finish_date).isoformat()
        else:
            filter_['FilterType'] = 3
            filter_['SkipFromLast'] = 0
        packets = self._make_request("MoApi2/GetMoPackets", 'Packets', Filter=filter_, timeit=timeit, debugit=debugit)
        df = pd.DataFrame.from_records(packets)
        records = []
        for packets in df['DataJson']:
            packets = json.loads(packets)
            records.append(dict(zip([self.value_types.get(packet['Id'], 'undefined') for packet in packets],
                                    [packet['Sum'] / packet['Cnt'] for packet in packets])))
        df = df.assign(**pd.DataFrame.from_records(records))
        df = prep_df(df.drop(['DataJson'], axis=1), index_col='date')
        return df

    def get_locations(self):
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

        locations_data = self._make_request(f"MoApi2/GetMoItems", "Locations")
        locations_data = prep_dicts(locations_data, RIGHT_PARAMS_NAMES, USELESS_COLS + ['LocationId'])
        for location_data in locations_data:
            location_data['stations'] = stations_by_location.get(location_data.get('name'))
        return locations_data
