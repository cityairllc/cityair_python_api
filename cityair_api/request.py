import requests
import pandas as pd
from pandas.io.json import json_normalize
import datetime
import time
import json
from collections.abc import Iterable
from collections import Counter

RIGHT_PARAMS_NAMES = {'FlagBatLow': 'BatLow', 'FlagPs220': '220', 'RecvDate': 'RecieveDate',
                      'SendDate': 'Date', 'Temperature': 'T',
                      'Humidity': 'RH', 'Pressure': 'P'}


class CityAirRequest:

    def __init__(self, user, psw, **kwargs):
        self.timeout = kwargs.get('timeout', 100)
        self.silent = kwargs.get('silent')
        self.main_url = kwargs.get('main_url', "http://185.171.100.156:49106")
        if kwargs.get('dev'):
            self.request_url = self.request_url.replace("request.php", "request-dev.php")
        response = self.make_request(f"Auth/Init", User=user, Pwd=psw)
        self.token = response['Token']
        print(
            f"Welcome, {user}! You have access to {len(response['UserItem']['DeviceIds'])} devices and {len(
                response['UserItem']['MoIds'])} monitoring stations")

    def make_request(self, method_url, **kwargs):
        body = {"Token": getattr(self, 'token', None), **kwargs}
        url = f"{self.main_url}/{method_url}"
        response = requests.post(url, json=body, timeout=self.timeout).json()
        if response['IsError']:
            raise Exception(
                f"Error while getting data: \n"
                f"url: {url}\n"
                f"request: {str(body)}\n"
                f"{response['ErrorMessage']}:\n"
                f"{response['ErrorMessageDetals']}")
        return response['Result']

    def get_devices(self, filter_offline=False):
        response = r.make_request(f"DevicesApi2/GetDevices")
        _ = pd.DataFrame.from_records(response["PacketsValueTypes"])
        self.value_types = dict(zip(_['ValueType'], _['TypeName']))
        res = pd.DataFrame.from_records(response["Devices"])
        if filter_offline:
            res = res[~ res['IsOffline']]
        self.device_serials = dict(zip(res['DeviceId'], res['SerialNumber']))
        self.device_ids = dict(zip(res['SerialNumber'], res['DeviceId']))
        res.set_index('SerialNumber', inplace=True)

        for children in res['ChildDevices']:
            for child in children:
                self.device_serials[child.get('DeviceId')] = child.get('SerialNumber')
        return res

    def get_device_data(self, serial_number, start_date=None, finish_date=datetime.datetime.now(), take_count=1000,
                        all_cols=False):
        try:
            device_id = self.device_ids[serial_number]
        except AttributeError:
            self.get_devices()
            device_id = self.device_ids[serial_number]
        except KeyError:
            raise Exception(f"You don't have permission to the device with serial {serial_number}")
        filter_ = {'Take': take_count, 'DeviceId': device_id}
        if start_date:
            filter_['FilterType'] = 1
            filter_['TimeBegin'] = self.to_date(start_date)
            filter_['TimeEnd'] = self.to_date(finish_date)
        else:
            filter_['FilterType'] = 3
            filter_['Skip'] = 0
        packets = self.make_request("DevicesApi2/GetPackets", Filter=filter_)['Packets']
        df = pd.DataFrame.from_records(packets)
        df.drop(['DataJson', 'PacketId'], 1, inplace=True)
        if not all_cols:
            df.drop(['FlagBatLow', 'FlagPs220', 'RecvDate', 'GeoInfo'], axis=1, inplace=True, errors='ignore')
        else:
            # unpacking columns that are dictionaries
            columns_to_unpack = ['GeoInfo']
            for col in columns_to_unpack:
                df = df.assign(**df[col].apply(pd.Series)).drop(col, 1)
                # unpacking columns that are list of dict (['Data'])
        records = []
        for packets in df['Data']:
            records.append(dict(zip(
                [f"{self.value_types[packet['VT']]} {self.device_serials[packet['D']]}" for packet in packets],
                [packet['V'] for packet in packets])))
        df = df.assign(**pd.DataFrame.from_records(records))
        # now renaming columns if there is only one valuetype from this device. else leaving serial as suffix
        param_names = dict(zip([col.split(' ')[0] for col in df], [set() for _ in df]))
        for col in df:
            param_names[col.split(' ')[0]].add(col)
        for param in param_names:
            if len(param_names[param]) == 1:
                df.rename({param_names[param].pop(): param}, inplace=True, axis=1)
        # dropping useless and empty columns and renaming to proper names
        df.drop(['Data'], 1, inplace=True)
        for date_col in list(filter(lambda col: col.endswith('Date'), df.columns)):
            df[date_col] = df[date_col].apply(pd.to_datetime)
        df.rename(RIGHT_PARAMS_NAMES, inplace=True, axis=1)

        df.set_index('Date', inplace=True)
        df.dropna(how='all', axis=1, inplace=True)
        return df

    @staticmethod
    def to_date(date_string):
        if isinstance(date_string, datetime.datetime):
            return date_string.isoformat()
        else:
            try:
                return pd.to_datetime(date_string, dayfirst=True).isoformat()
            except Exception:
                raise Exception("Wrong date format")

