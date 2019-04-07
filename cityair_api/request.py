import requests
import pandas as pd
from pandas.io.json import json_normalize
import datetime
import time
import json


class CityAirRequest:
    def __init__(self, user, psw, dev=False, request_timeout=5):
        self.user = user
        self.psw = psw
        if dev:
            self.is_dev = True
            self.request_url = f'https://cityair.io/backend-api/request-dev.php?map=/'
            self.device_data_url = f'{self.request_url}DevicesApi/GetPackets'
            self.device_data_url_raw = f'{self.request_url}DevicesApi/GetPackets'
        else:
            self.is_dev = False
            self.request_url = f'https://cityair.io/backend-api/request.php?map=/'
            self.device_data_url = f'{self.request_url}DevicesApi/GetPackets'
            self.device_data_url_raw = f'{self.request_url}DevicesApiRaw/GetPackets'
        body = {"Auth": {"User": self.user,
                         "Pwd": self.psw},
                "Filter": {}}
        self.devices_url = f'{self.request_url}DevicesApi/GetDevices'
        self.devices_url_raw = f'{self.request_url}DevicesApiRaw/GetDevices'
        self.stations_url = f'{self.request_url}MonitoringStationsApi/GetMonitoringStations'
        self.station_data_url = f'{self.request_url}MonitoringStationsApi/GetMonitoringStationPackets'
        self.request_timeout = request_timeout
        self.right_cols = {
            'temperature': 'T',
            'humidity': 'RH',
            'pm2': 'PM2.5',
            'pm10': 'PM10',
            'pressure': 'P'
        }
        # check authentication

        url = self.devices_url
        try:
            response = requests.post(url, json=body, timeout=self.request_timeout)
            response.raise_for_status()
            if response.json()['IsError']:
                raise Exception(f"{response.json()['ErrorMessage']}:\n{response.json()['ErrorMessageDetals']}")
            else:
                tmp = json_normalize(response.json()['Result']['Devices'])
                self.device_ids = pd.Series(data=tmp["DeviceId"], name="device_ids")
                self.device_ids.index = tmp["SerialNumber"]
                # print(f"Welcome, {user}. You have {len(self.device_ids)} devices available!")
        except Exception as e:
            raise Exception(
                f"Error while getting device list:\n"
                f" {e.__str__()}\n"
                f"url : {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")

    def get_devices(self, full_info=False, raw=False):

        body = {"Auth": {"User": self.user,
                         "Pwd": self.psw},
                "Filter": {}}
        if raw:
            url = self.devices_url_raw
        else:
            url = self.devices_url
        try:
            response = requests.post(url, json=body, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.Timeout:
            raise Exception(f"Я выждал {self.request_timeout} секунд, но сервер не ответил. Странно.")
        except Exception as e:
            raise Exception(
                f"Error while getting device list:\n"
                f" {e.__str__()}\n"
                f"url : {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        try:
            df_raw = json_normalize(response.json()['Result']['Devices'])
            if raw:
                df_raw.index = df_raw['SerialNumber']
                return df_raw
            if full_info:
                df = pd.DataFrame()
                df['Имя'] = df_raw['Name']
                df['Онлайн'] = df_raw['IsOffline'].apply(
                    lambda online: "неизвестно" if online is None else '<b>Нет</b>' if online else 'Да')
                df['Время последнего пакета'] = df_raw['DeviceLastPacket'].apply(
                    lambda date_string: None if date_string is None else pd.to_datetime(date_string))
                df['Питание 220В'] = df_raw['FlagPs220'].apply(
                    lambda ps220: None if ps220 is None else 'Есть' if ps220 else '<b>Нет</b>')
                df['Батарея заряжена'] = df_raw['FlagBatLow'].apply(
                    lambda batlow: None if batlow is None else '<b>Нет</b>' if batlow else 'Да')
                df['Версия прошивки'] = df_raw['SoftwareVersion'].apply(
                    lambda firmware: None if firmware is None else firmware)
                df['Дата запуска'] = df_raw['DeviceWorkBegin'].apply(lambda
                                                                         date_string: None if date_string is None else f"{pd.to_datetime(date_string).strftime('%d.%m.%Y')}")
                df.index = df_raw['SerialNumber']
                df.index.name = 'S/N'
                return df
            else:
                return list(df_raw.SerialNumber)
        except Exception as e:
            raise Exception(
                f"Error while getting device list:\n"
                f"{e.__str__()}\n"
                f"url : {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}\n"
                f"response {response.json()}")

    def get_last_packet(self, serial_number):
        body = {"Auth":
                    {"User": self.user,
                     "Pwd": self.psw},
                "Filter": {
                    'FilterType': 1,
                    "DeviceId": f'{self.device_ids[serial_number]}',
                    "MaxPacketsCount": 1,
                    "Skip": 0
                }}
        url = f'https://cityair.io/backend-api/request.php?map=/DevicesApi/GetPackets'
        try:
            response = requests.post(url, json=body, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.Timeout:
            raise Exception(
                f"Я выждал {self.request_timeout} секунд, но сервер не ответил. "
                f"Вероятно, станция давно не выходила на связь.")
        except Exception as e:
            raise Exception(
                f"Error while getting {serial_number} device last packet: \n"
                f"{e.__str__()}\n"
                f"url : {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        try:
            params_to_throw = ['IsSendDateReal', 'PacketId', 'DeviceId','StationId', 'IsSendDateReal', 'Tag',
                               'GeoInfo']
            last_packet = response.json()['Result']['Packets'][0]
            res = dict([(param, last_packet[param]) if (last_packet[param] and param not in params_to_throw) else (
                'SendDate', last_packet['SendDate']) for param in last_packet])
            return res
        except Exception as e:
            raise Exception(
                f"Error while getting {serial_number} device last packet: \n"
                f"{e.__str__()}\n"
                f"url : {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}\n"
                f"response {response.json()}")

    def get_device_data(self, serial_number, start_date=datetime.datetime.now() - datetime.timedelta(hours=1),
                        finish_date=datetime.datetime.now(), utc_hour_dif=7, print_response=True, print_json=False, raw = False):
        start_date = self.to_date(start_date) - datetime.timedelta(hours=utc_hour_dif)
        finish_date = self.to_date(finish_date) - datetime.timedelta(hours=utc_hour_dif)
        if raw:
            url = self.device_data_url_raw
        else:
            url = self.device_data_url

        body = {"Auth": {"User": self.user,
                         "Pwd": self.psw},
                "Filter": {
                    "FilterType": 2,
                    "BeginTime": start_date.isoformat(),
                    "EndTime": finish_date.isoformat(),
                    "DeviceId": f'{self.device_ids[serial_number]}',
                    "MaxPacketsCount": 10 ** 6,
                    "Skip": 0}}
        start_time = time.time()
        try:
            response = requests.post(url, json=body, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.Timeout:
            raise Exception(
                f"Я выждал {self.request_timeout} секунд, но сервер не ответил. "
                f"Вероятно, станция давно не выходила на связь.")
        except Exception as e:
            raise Exception(
                f"Error while getting {serial_number} device data: \n"
                f"{e.__str__()}\n"
                f"url: {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        elapsed_time = time.time() - start_time
        if print_json:
            print(f"url : {url}\nbody: {body}\nresponse {response.json()}")
        try:
            df = json_normalize(response.json()['Result']['Packets'])
        except Exception as e:
            try:
                raise Exception(
                    f"Error while getting {serial_number} device data:\n"
                    f"{response.json()['ErrorMessage']}\n"
                    f"{response.json()['ErrorMessageDetals']}\n"
                    f"{e.__str__()}\n"
                    f"url: {url}\n"
                    f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
            except Exception:
                raise Exception(
                    f"Error while getting {serial_number} device data:\n"
                    f"{e.__str__()}\n"
                    f"url: {url}\n"
                    f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}\n"
                    f"response: {response.json()}")

        if len(df.index) == 0:
            if print_response:
                print(f"{serial_number}, {elapsed_time:.2f} s for {df.shape[0]} packets")
            return pd.DataFrame()
        try:
            df.index = pd.to_datetime(df.SendDate, dayfirst=True) + datetime.timedelta(hours=utc_hour_dif)
            df.index.name = 'Date'
            df.sort_index(axis=0, level=None, ascending=True, inplace=True)
            df.columns = [self.right_cols[col.lower()] if col.lower() in self.right_cols else col for col in df.columns]
            if print_response:
                print(f"{serial_number}, {elapsed_time:.2f} s for {df.shape[0]} packets")
           # df.drop(['SendDate'], axis=1, inplace=True)
        except Exception as e:
            raise Exception(
                f"Error while getting {serial_number} device data: \n"
                f"error with dataframe processing: {e.__str__()}\n"
                f"url: {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}\n"
                f"response: {response.json()}")
        return df

    def get_device_lastweek_data(self, serial_number):
        try:
            finish_date = pd.to_datetime(self.get_last_packet(serial_number)['SendDate'])
            start_date = finish_date - datetime.timedelta(days=7)
            return self.get_device_data(serial_number, start_date, finish_date, print_json=False)
        except Exception as e:
            return pd.DataFrame()

    def get_devices_data(self, serial_numbers, start_date=datetime.datetime.now() - datetime.timedelta(hours=1),
                         finish_date=datetime.datetime.now(), param='PM2.5', utc_hour_dif=7, print_response=True):
        df = pd.DataFrame()
        for serial_number in serial_numbers:
            try:
                new_series = self.get_device_data(serial_number, start_date, finish_date, utc_hour_dif, print_response)[
                    param].resample('1T').mean()
            except Exception as e:
                print(e.__str__())
                new_series = pd.Series()
            new_series.name = serial_number
            df = pd.concat([df, new_series], axis=1)
        return df

    def get_stations(self, full_info=False, print_json=False):
        body = {"Auth":
                    {"User": self.user,
                     "Pwd": self.psw}}
        url = self.stations_url
        try:
            response = requests.post(url, json=body, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.Timeout:
            raise Exception(
                f"Я выждал {self.request_timeout} секунд, но сервер не ответил. Очень странно")
        except Exception as e:
            raise Exception(
                f"Error while getting stations info:\n"
                f"{e.__str__()}\n"
                f"url: {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        try:
            if print_json:
                print(f"url : {url}\nbody: {body}\nresponse {response.json()}")
            df = json_normalize(response.json()['Result']['MonitoringStations'])
            if full_info:
                df.index = df.MonitoringObjectId
                return df
            else:
                res = {}
                for i in range(df.shape[0]):  # series в dict ??
                    res[df['MonitoringObjectId'][i]] = df['PublishNameRU'][i]
                return res
        except Exception as e:
            print(
                f"Error while getting stations info:\n"
                f"{e.__str__()}\n"
                f"url: {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")

    def get_station_data(self, station_id,
                         start_date=None,
                         finish_date=datetime.datetime.now(), period='5min', utc_hour_dif=0, print_response=True,
                         print_json=False,
                         params=None):
        time_periods = {'5min': 1, '20min': 2, '1hr': 3, '24hr': 4}
        if start_date:
            finish_date = self.to_date(finish_date) - datetime.timedelta(hours=utc_hour_dif)
            start_date = self.to_date(start_date) - datetime.timedelta(hours=utc_hour_dif)
            body = {"Auth":
                    {"User": self.user,
                     "Pwd": self.psw},
                    "Filter": {
                        "MonitoringObjectId": f"{station_id}",
                        "FilterType": 1,
                        "IntervalType": time_periods[period],
                        "BeginTime": start_date.isoformat(),
                        "EndTime": finish_date.isoformat()}}
        else:
            body = {"Auth":
                    {"User": self.user,
                     "Pwd": self.psw},
                    "Filter": {
                        "MonitoringObjectId": f"{station_id}",
                        "FilterType": 3,
                        "IntervalType": 1,
                        "SkipFromLast": 0,
                        "TakeCount": 2016
                    }}
        url = self.station_data_url
        start_time = time.time()
        try:
            response = requests.post(url, json=body, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.Timeout:
            raise Exception(
                f"Я выждал {self.request_timeout} секунд, но сервер не ответил. Очень странно")
        except Exception as e:
            raise Exception(
                f"Error while getting {station_id} station data:\n"
                f"{e.__str__()}\n"
                f"url: {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        request_time = time.time() - start_time
        if print_json:
            print(f"url : {url}\nbody: {body}\nresponse {response.json()}")
        try:
            tmp_df = json_normalize(response.json()['Result']['PacketItems'])
        except Exception as e:
            try:
                raise Exception(
                    f"Error while getting {station_id} station data:\n"
                    f"{response.json()['ErrorMessage']}\n"
                    f"{response.json()['ErrorMessageDetals']}\n"
                    f"{e.__str__()}\n"
                    f"url: {url}\n"
                    f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
            except Exception:
                raise Exception(
                    f"Error while getting {station_id} station data:\n"
                    f"{e.__str__()}\n"
                    f"url: {url}\n"
                    f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}\n"
                    f"response: {response.json()}")
        try:
            if len(tmp_df.index) == 0:
                if print_response:
                    print(f"mo_id: {station_id}, packets_count: {tmp_df.shape[0]}. took {request_time:.2f}s to collect")
                return pd.DataFrame(columns=params if params else ['PM2.5', 'PM10', 'T', 'RH', 'P'])
            df = pd.DataFrame()
            for data_str in tmp_df['DataJson']:
                df = df.append(dict([(param['Id'], param['Sum'] / param['Cnt']) for param in json.loads(data_str)]),
                               ignore_index=True)

            df.index = pd.to_datetime(tmp_df['SendDate'], dayfirst=True) + datetime.timedelta(hours=utc_hour_dif)
            df.index.name = 'Date'

            #  переименовываю колонки, названия из запроса для станций
            d = dict([(param['PacketParamLinkId'], param['ParamName']) for param in
                      self.get_stations(full_info=True)['PacketParamLinkItems'][station_id]])
            df.columns = [d[param_id] for param_id in df.columns]
            # переименоываю, чтоб красивее было
            df.columns = [self.right_cols[col.lower()] if col.lower() in self.right_cols else col for col in df.columns]
            df.sort_index(axis=0, level=None, ascending=True, inplace=True)
            processing_time = time.time() - start_time - request_time
            if print_response:
                print(
                    f"mo_id: {station_id}, packets_count: {df.shape[0]}. "
                    f"took {request_time:.2f}s to collect,  {processing_time:.2f}s to process")
            if params:
                return df[params]
            return df
        except Exception as e:
            raise Exception(
                f"Error while getting {station_id} station data:\n"
                f"failed during dataframe processing: {e.__str__()}\n"
                f"url: {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}\n"
                f"response: {response.json()}")

    def get_stations_data(self, station_ids,
                          start_date=None,
                          finish_date=datetime.datetime.now(), period='5min', param='PM2.5', utc_hour_dif=7,
                          print_response=True):
        df = pd.DataFrame()
        for station_id in station_ids:
            try:
                new_series = \
                    self.get_station_data(station_id, start_date, finish_date, period, utc_hour_dif, print_response)[
                        param]

            except Exception:
                new_series = pd.Series()
            new_series.name = station_id
            df = pd.concat([df, new_series], axis=1)
        df.columns = station_ids
        mo_names = self.get_stations()
        df.columns = [mo_names[mo_id] for mo_id in df.columns]
        return df


    @staticmethod
    def to_date(date_string):
        if isinstance(date_string, datetime.datetime):
            return date_string
        else:
            try:
                return pd.to_datetime(date_string, dayfirst=True)
            except Exception:
                raise Exception("Wrong date format")
