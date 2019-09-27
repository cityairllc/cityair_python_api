import pandas as pd
import datetime
from collections import Counter
from .utils import to_date, timeit, debugit
from .exceptions import *

RIGHT_PARAMS_NAMES = {'FlagBatLow': 'BatLow', 'FlagPs220': '220', 'RecvDate': 'RecieveDate',
                      'SendDate': 'Date', 'Temperature': 'T',
                      'Humidity': 'RH', 'Pressure': 'P'}
DEFAULT_HOST = "https://develop.cityair.io/backend-api/request-dev-pg.php?map="


class CityAirRequest:
    def __init__(self, user, psw, **kwargs):
        self.host_url = kwargs.get('host_url', DEFAULT_HOST)
        self.timeout = kwargs.get('timeout', 100)
        self.user = user
        self.psw = psw

    @debugit
    @timeit
    def _make_request(self, method_url, *keys, **kwargs):
        body = {"User": getattr(self, 'user'), "Pwd": getattr(self, 'psw'), **kwargs}
        url = f"{self.host_url}/{method_url}"
        response = requests.post(url, json=body, timeout=self.timeout)
        if response.json()['IsError']:
            raise ServerException(response)
        response_data = response.json()['Result']
        for key in keys:
            if len(response_data[key]) == 0:
                raise EmptyDataException(response)
        if len(keys) == 0:
            return response_data
        elif len(keys) == 1:
            return response_data[keys[0]]
        else:
            return [response_data[key] for key in keys]

    @timeit
    def get_devices(self, format='list', include_offline=True, include_children=False, **kwargs):
        value_types_data, devices_data = self._make_request(f"DevicesApi2/GetDevices", "PacketsValueTypes", "Devices",
                                                            **kwargs)
        value_types_data = pd.DataFrame.from_records(value_types_data)
        self.value_types = dict(zip(value_types_data['ValueType'], value_types_data['TypeName']))
        df = pd.DataFrame.from_records(devices_data)
        df.index = df['SerialNumber']
        if not include_offline:
            df = df[~ df['IsOffline']]
        if format == 'dict':
            res = []
            for serial in df.index:
                info = df.loc[serial]
                res.append({'serial_number': info['SerialNumber'],
                            'name': info['DeviceName']})
                if include_children:
                    res[-1]['children'] = []
                    for child in info['ChildDevices']:
                        res[-1]['children'].append({'serial_number': child['SerialNumber'],
                                                    'name': child['DeviceName']})
            return res
        # saving dict to idntify device by id
        self.device_serials = dict(zip(df['DeviceId'], df['SerialNumber']))
        self.device_ids = dict(zip(df['SerialNumber'], df['DeviceId']))
        for children in df['ChildDevices']:
            if children:
                for child in children:
                    self.device_serials[child.get('DeviceId')] = child.get('SerialNumber')

        if include_children:
            for i in range(df.shape[0]):
                line = df.iloc[i]
                for device in line['ChildDevices']:
                    df = df.append(device, ignore_index=True)
        df.index = df['SerialNumber']
        if format == 'list':
            return df.index
        if format == 'raw':
            return df
        else:
            raise Exception(
                f"Unknown type of fromat arqument: {format}. Available formats are: list, raw, dict")

    @timeit
    def get_device_data(self, serial_number, start_date=None,
                        finish_date=datetime.datetime.now(),
                        take_count=1000, all_cols=False,
                        separate_device_data=False, **kwargs):
        def finilize_df(df, all_cols=all_cols):
            cols_to_drop = ['220', 'BatLow', 'RecieveDate', 'GeoInfo', 'Date', 'SendDate', 'Latitude', 'Longitude']
            df.rename(RIGHT_PARAMS_NAMES, inplace=True, axis=1)
            df.dropna(how='all', axis=1, inplace=True)
            if not all_cols:
                df.drop(cols_to_drop, axis=1, inplace=True, errors='ignore')
            return df

        try:
            device_id = self.device_ids[serial_number]
        except AttributeError:
            self.get_devices()
            device_id = self.device_ids[serial_number]
        except KeyError:
            raise NoAccessException(f"You don't have permission to the device with serial {serial_number}")
        filter_ = {'Take': take_count, 'DeviceId': device_id}
        if start_date:
            filter_['FilterType'] = 1
            filter_['TimeBegin'] = to_date(start_date).isoformat()
            filter_['TimeEnd'] = to_date(finish_date).isoformat()
        else:
            filter_['FilterType'] = 3
            filter_['Skip'] = 0
        packets = self._make_request("DevicesApi2/GetPackets", 'Packets', Filter=filter_, **kwargs)
        df = pd.DataFrame.from_records(packets)
        df.drop(['DataJson', 'PacketId'], 1, inplace=True, errors='ignore')

        columns_to_unpack = ['GeoInfo']
        for col in columns_to_unpack:
            df = df.assign(**df[col].apply(pd.Series)).drop(col, 1)
            # unpacking columns that are list of dict (['Data'])
        records = []
        for packets in df['Data']:
            records.append(dict(zip(
                [f"value {packet['D']} {packet['VT']}" for packet in packets],
                [packet['V'] for packet in packets])))
        df = df.assign(**pd.DataFrame.from_records(records))
        for date_col in list(filter(lambda col: col.endswith('Date'), df.columns)):
            df[date_col] = df[date_col].apply(to_date)
        df.index = df['SendDate'].rename('Date')
        df.drop('SendDate', axis=1, inplace=True)

        values_cols = list(filter(lambda col: col.startswith('value'), df.columns))
        if separate_device_data:
            res = dict()
            for col in values_cols:
                _, device_id, value_id = col.split(' ')
                serial = self.device_serials[int(device_id)]
                value_name = self.value_types[int(value_id)]
                series_to_append = df[col].rename(value_name)
                try:
                    res[serial] = pd.concat([res[serial], series_to_append], axis=1)
                except KeyError:
                    res[serial] = series_to_append.to_frame()
            try:
                res[serial_number] = pd.concat([df.drop(values_cols + ['Data'], axis=1), res[serial_number]], axis=1)
            except KeyError:
                res[serial_number] = df.drop(values_cols + ['Data'], axis=1)
            for device in res:
                res[device] = finilize_df(res[device])
            return res
        else:
            value_types_count = Counter(list(
                map(lambda s: (s.split(' ')[-1]), values_cols)))
            for col in list(filter(lambda col: col.startswith('value'), df.columns)):
                _, device_id, value_id = col.split(' ')
                serial = self.device_serials[int(device_id)]
                value_name = self.value_types[int(value_id)]
                if value_types_count[value_id] > 1:
                    proper_col_name = f"{value_name} [{serial}]"
                else:
                    proper_col_name = f"{value_name}"
                df.rename(columns={col: proper_col_name}, inplace=True)
            return finilize_df(df.drop(['Data'], axis=1))
