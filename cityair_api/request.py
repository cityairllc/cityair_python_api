import requests
import pandas as pd
from pandas.io.json import json_normalize
import datetime
import time
import json


class CityAirRequest:

    def __init__(self, user, psw, dev=False, raw=False, request_timeout=5, silent=False, debug=False):
        self.user = user
        self.psw = psw
        self.request_timeout = request_timeout
        self.debug = debug
        self.silent = silent
        self.right_cols = {
            'temperature': 'T',
            'humidity': 'RH',
            'pm2': 'PM2.5',
            'pm10': 'PM10',
            'pressure': 'P'
        }

        self.request_url = f'https://cityair.io/backend-api/request.php?map=/'
        if dev:
            self.request_url = self.request_url.replace(
                "request.php", "request-dev.php")
        self.device_data_url = f'{self.request_url}DevicesApi/GetPackets'
        self.devices_url = f'{self.request_url}DevicesApi/GetDevices'
        self.stations_url = f'{self.request_url}MonitoringStationsApi/GetMonitoringStations'
        self.station_data_url = f'{self.request_url}MonitoringStationsApi/GetMonitoringStationPackets'
        if raw:
            self.devices_url = self.devices_url.replace("DevicesApi", "DevicesApiRaw")
            self.device_data_url = self.device_data_url.replace("DevicesApi", "DevicesApiRaw")
        self.device_serials = self.get_devices(format='series')
        print(
            f"Welcome, {user}. You have {len(self.device_serials)} devices available!")

    def make_request(self, url, filter_, desciption, silent=True):
        body = {"Auth": {"User": self.user,
                         "Pwd": self.psw},
                "Filter": filter_}
        start_time = time.time()
        if self.debug:
            print(f"request_url: {url}\nrequest: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        try:
            response = requests.post(
                url, json=body, timeout=self.request_timeout)
            response.raise_for_status()
            if self.debug:
                print(f"response: {response.json()}")
        except requests.Timeout:
            raise Exception(
                f"Server didn't respond in {self.request_timeout} seconds")
        except Exception as e:
            raise Exception(
                f"Error while getting data: \n"
                f"{e.__str__()}\n"
                f"url: {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        if response.json()['IsError']:
            raise Exception(
                f"{response.json()['ErrorMessage']}:\n{response.json()['ErrorMessageDetals']}")
        df = json_normalize(response.json()['Result'][desciption])
        if not silent:
            try:
                id_ = filter_[list(filter(lambda key: 'Id' in key, list(filter_.keys())))[0]]
            except IndexError:
                id_ = 'undefined'
            print(f"Got {id_} data of shape {df.shape} for {time.time() - start_time:.2f} seconds")
        return df

    def get_devices(self, format='series', silent=True):
        df = self.make_request(self.devices_url, {}, 'Devices', silent)

        if format == 'series':
            if len(df.index) == 0:
                return pd.Series(name='SerialNumber')
            return pd.Series(data=list(df['SerialNumber']), index=df['DeviceId']).dropna()
        elif format == 'list':
            return list(df['SerialNumber'])
        elif format == 'raw':
            return df
        elif format == 'pretty':
            df_pretty = pd.DataFrame()
            if len(df.index) == 0:
                return df_pretty
            df_pretty['Имя'] = df['Name']
            df_pretty['Онлайн'] = df['IsOffline'].apply(
                lambda online: "неизвестно" if online is None else '<b>Нет</b>' if online else 'Да')
            df_pretty['Время последнего пакета'] = df['DeviceLastPacket'].apply(
                lambda date_string: None if date_string is None else pd.to_datetime(date_string))
            df_pretty['Питание 220В'] = df['FlagPs220'].apply(
                lambda ps220: None if ps220 is None else 'Есть' if ps220 else '<b>Нет</b>')
            df_pretty['Батарея заряжена'] = df['FlagBatLow'].apply(
                lambda batlow: None if batlow is None else '<b>Нет</b>' if batlow else 'Да')
            df_pretty['Версия прошивки'] = df['SoftwareVersion'].apply(
                lambda firmware: None if firmware is None else firmware)
            df_pretty['Дата запуска'] = df['DeviceWorkBegin'].apply(lambda
                                                                        date_string: None if date_string is None else f"{pd.to_datetime(date_string).strftime('%d.%m.%Y')}")
            df_pretty.index = df['SerialNumber']
            df_pretty.index.name = 'S/N'
            return df_pretty
        else:
            raise Exception(
                f"Unknown type of request: {format}. Available format requests are: series, list, raw, pretty")

    def get_last_packet(self, serial_number, silent=True, only_date = False):
        filter_ = {
            'FilterType': 1,
            "DeviceId": f'{(self.device_serials[self.device_serials==serial_number].index)[0]}',
            "MaxPacketsCount": 1,
            "Skip": 0
        }
        last_packet = dict(self.make_request(
            self.device_data_url, filter_, 'Packets', True).iloc[0], silent=silent)
        if only_date:
            return pd.to_datetime(last_packet['SendDate'])
        params_to_throw = ['IsSendDateReal', 'PacketId', 'DeviceId', 'StationId', 'IsSendDateReal', 'Tag',
                           'GeoInfo.Latitude', 'GeoInfo.Longitude']
        return dict([(param, last_packet[param]) if (last_packet[param] and param not in params_to_throw) else (
            'SendDate', last_packet['SendDate']) for param in last_packet])

    def get_device_data(self, serial_number, start_date=None,
                        finish_date=None, utc_hour_dif=7, max_packets_count=10000, silent=False):
        if finish_date:
            finish_date = self.to_date(
                finish_date) - datetime.timedelta(hours=utc_hour_dif)
        else:
            finish_date = datetime.datetime.now()
        if start_date:
            start_date = self.to_date(start_date) - \
                         datetime.timedelta(hours=utc_hour_dif)
            filter_ = {
                "FilterType": 2,
                "BeginTime": start_date.isoformat(),
                "EndTime": finish_date.isoformat(),
                "DeviceId": f'{(self.device_serials[self.device_serials==serial_number].index)[0]}',
                "MaxPacketsCount": max_packets_count,
                "Skip": 0}
        else:
            filter_ = {
                'FilterType': 1,
                "DeviceId": f'{(self.device_serials[self.device_serials==serial_number].index)[0]}',
                "MaxPacketsCount": max_packets_count,
                "Skip": 0
            }

        df = self.make_request(self.device_data_url,
                               filter_, 'Packets', silent=silent)
        df.index = pd.to_datetime(
            df.SendDate, dayfirst=True) + datetime.timedelta(hours=utc_hour_dif)
        df.index.name = 'Date'
        df.sort_index(axis=0, level=None, ascending=True, inplace=True)
        df.columns = [self.right_cols[col.lower()] if col.lower(
        ) in self.right_cols else col for col in df.columns]
        return df.dropna(axis=1, how='all')

    def get_devices_data(self, *serial_numbers, start_date=None,
                         finish_date=None, param='PM2.5', utc_hour_dif=7, max_packets_count=10000, silent=False):
        df = pd.DataFrame()
        for serial_number in serial_numbers:
            try:
                new_series = self.get_device_data(serial_number, start_date=start_date, finish_date=finish_date,
                                                  utc_hour_dif=utc_hour_dif,
                                                  max_packets_count=max_packets_count, silent=silent)[param].resample(
                    '1T').mean()
            except Exception as e:
                print(e.__str__())
                new_series = pd.Series()
            new_series.name = serial_number
            df = pd.concat([df, new_series], axis=1)
        return df

    def get_stations(self, full_info=False, silent=False):
        filter_ = {}
        df = self.make_request(self.stations_url, filter_,
                               'MonitoringStations', silent=silent)
        df.index = df.MonitoringObjectId
        if full_info:
            return df
        else:
            return df['Name']

    def get_station_data(self, station_id,
                         start_date=None,
                         finish_date=datetime.datetime.now(), period='5min', silent=False):
        time_periods = {'5min': 1, '20min': 2, '1hr': 3, '24hr': 4}
        if start_date:
            finish_date = self.to_date(finish_date)
            start_date = self.to_date(start_date)
            filter_ = {
                "MonitoringObjectId": f"{station_id}",
                "FilterType": 1,
                "IntervalType": time_periods[period],
                "BeginTime": start_date.isoformat(),
                "EndTime": finish_date.isoformat()}
        else:
            filter_ = {
                "MonitoringObjectId": f"{station_id}",
                "FilterType": 3,
                "IntervalType": 1,
                "SkipFromLast": 0,
                "TakeCount": 2016
            }
        tmp_df = self.make_request(
            self.station_data_url, filter_, 'PacketItems', silent=silent)
        if len(tmp_df.index) == 0:
            return pd.DataFrame(columns=['PM2.5', 'PM10', 'T', 'RH', 'P'])
        df = pd.DataFrame()
        for data_str in tmp_df['DataJson']:
            df = df.append(dict([(param['Id'], param['Sum'] / param['Cnt']) for param in json.loads(data_str)]),
                           ignore_index=True)
        df.index = pd.to_datetime(tmp_df['SendDate'], dayfirst=True)
        df.index.name = 'Date'
        #  переименовываю колонки, названия из запроса для станций
        d = dict([(param['PacketParamLinkId'], param['ParamName']) for param in
                  self.get_stations(full_info=True, silent=True)['PacketParamLinkItems'][station_id]])
        df.columns = [d[param_id] for param_id in df.columns]
        # переименоываю, чтоб красивее было
        df.columns = [self.right_cols[col.lower()] if col.lower(
        ) in self.right_cols else col for col in df.columns]
        df.sort_index(axis=0, level=None, ascending=True, inplace=True)
        return df

    def get_stations_data(self, *station_ids,
                          start_date=None,
                          finish_date=datetime.datetime.now(), period='5min', param='PM2.5', silent=False):
        df = pd.DataFrame()
        for station_id in station_ids:
            try:
                new_series = \
                    self.get_station_data(
                        station_id, start_date, finish_date, period, silent=silent)[param]
            except Exception:
                new_series = pd.Series()
            new_series.name = station_id
            df = pd.concat([df, new_series], axis=1)
        df.columns = station_ids
        mo_names = self.get_stations(silent=True)
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
import requests
import pandas as pd
from pandas.io.json import json_normalize
import datetime
import time
import json


class CityAirRequest:

    def __init__(self, user, psw, dev=False, raw=False, request_timeout=5, silent=False, debug=False):
        self.user = user
        self.psw = psw
        self.request_timeout = request_timeout
        self.debug = debug
        self.silent = silent
        self.right_cols = {
            'temperature': 'T',
            'humidity': 'RH',
            'pm2': 'PM2.5',
            'pm10': 'PM10',
            'pressure': 'P'
        }

        self.request_url = f'https://cityair.io/backend-api/request.php?map=/'
        if dev:
            self.request_url = self.request_url.replace(
                "request.php", "request-dev.php")
        self.device_data_url = f'{self.request_url}DevicesApi/GetPackets'
        self.devices_url = f'{self.request_url}DevicesApi/GetDevices'
        self.stations_url = f'{self.request_url}MonitoringStationsApi/GetMonitoringStations'
        self.station_data_url = f'{self.request_url}MonitoringStationsApi/GetMonitoringStationPackets'
        if raw:
            self.devices_url = self.devices_url.replace("DevicesApi", "DevicesApiRaw")
            self.device_data_url = self.device_data_url.replace("DevicesApi", "DevicesApiRaw")
        self.device_serials = self.get_devices(format='series')
        print(
            f"Welcome, {user}. You have {len(self.device_serials)} devices available!")

    def make_request(self, url, filter_, desciption, silent=True):
        body = {"Auth": {"User": self.user,
                         "Pwd": self.psw},
                "Filter": filter_}
        start_time = time.time()
        if self.debug:
            print(f"request_url: {url}\nrequest: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        try:
            response = requests.post(
                url, json=body, timeout=self.request_timeout)
            response.raise_for_status()
            if self.debug:
                print(f"response: {response.json()}")
        except requests.Timeout:
            raise Exception(
                f"Server didn't respond in {self.request_timeout} seconds")
        except Exception as e:
            raise Exception(
                f"Error while getting data: \n"
                f"{e.__str__()}\n"
                f"url: {url}\n"
                f"request: {str(body).replace(self.psw, '***').replace(self.user, '***')}")
        if response.json()['IsError']:
            raise Exception(
                f"{response.json()['ErrorMessage']}:\n{response.json()['ErrorMessageDetals']}")
        df = json_normalize(response.json()['Result'][desciption])
        if not silent:
            try:
                id_ = filter_[list(filter(lambda key: 'Id' in key, list(filter_.keys())))[0]]
            except IndexError:
                id_ = 'undefined'
            print(f"Got {id_} data of shape {df.shape} for {time.time() - start_time:.2f} seconds")
        return df

    def get_devices(self, format='series', silent=True):
        df = self.make_request(self.devices_url, {}, 'Devices', silent)

        if format == 'series':
            if len(df.index) == 0:
                return pd.Series(name='SerialNumber')
            return pd.Series(data=list(df['SerialNumber']), index=df['DeviceId']).dropna()
        elif format == 'list':
            return list(df['SerialNumber'])
        elif format == 'raw':
            return df
        elif format == 'pretty':
            df_pretty = pd.DataFrame()
            if len(df.index) == 0:
                return df_pretty
            df_pretty['Имя'] = df['Name']
            df_pretty['Онлайн'] = df['IsOffline'].apply(
                lambda online: "неизвестно" if online is None else '<b>Нет</b>' if online else 'Да')
            df_pretty['Время последнего пакета'] = df['DeviceLastPacket'].apply(
                lambda date_string: None if date_string is None else pd.to_datetime(date_string))
            df_pretty['Питание 220В'] = df['FlagPs220'].apply(
                lambda ps220: None if ps220 is None else 'Есть' if ps220 else '<b>Нет</b>')
            df_pretty['Батарея заряжена'] = df['FlagBatLow'].apply(
                lambda batlow: None if batlow is None else '<b>Нет</b>' if batlow else 'Да')
            df_pretty['Версия прошивки'] = df['SoftwareVersion'].apply(
                lambda firmware: None if firmware is None else firmware)
            df_pretty['Дата запуска'] = df['DeviceWorkBegin'].apply(lambda
                                                                        date_string: None if date_string is None else f"{pd.to_datetime(date_string).strftime('%d.%m.%Y')}")
            df_pretty.index = df['SerialNumber']
            df_pretty.index.name = 'S/N'
            return df_pretty
        else:
            raise Exception(
                f"Unknown type of request: {format}. Available format requests are: series, list, raw, pretty")

    def get_last_packet(self, serial_number, silent=True, only_date = False):
        filter_ = {
            'FilterType': 1,
            "DeviceId": f'{(self.device_serials[self.device_serials==serial_number].index)[0]}',
            "MaxPacketsCount": 1,
            "Skip": 0
        }
        last_packet = dict(self.make_request(
            self.device_data_url, filter_, 'Packets', True).iloc[0], silent=silent)
        if only_date:
            return pd.to_datetime(last_packet['SendDate'])
        params_to_throw = ['IsSendDateReal', 'PacketId', 'DeviceId', 'StationId', 'IsSendDateReal', 'Tag',
                           'GeoInfo.Latitude', 'GeoInfo.Longitude']
        return dict([(param, last_packet[param]) if (last_packet[param] and param not in params_to_throw) else (
            'SendDate', last_packet['SendDate']) for param in last_packet])

    def get_device_data(self, serial_number, start_date=None,
                        finish_date=None, utc_hour_dif=7, max_packets_count=10000, silent=False):
        if finish_date:
            finish_date = self.to_date(
                finish_date) - datetime.timedelta(hours=utc_hour_dif)
        else:
            finish_date = datetime.datetime.now()
        if start_date:
            start_date = self.to_date(start_date) - \
                         datetime.timedelta(hours=utc_hour_dif)
            filter_ = {
                "FilterType": 2,
                "BeginTime": start_date.isoformat(),
                "EndTime": finish_date.isoformat(),
                "DeviceId": f'{(self.device_serials[self.device_serials==serial_number].index)[0]}',
                "MaxPacketsCount": max_packets_count,
                "Skip": 0}
        else:
            filter_ = {
                'FilterType': 1,
                "DeviceId": f'{(self.device_serials[self.device_serials==serial_number].index)[0]}',
                "MaxPacketsCount": max_packets_count,
                "Skip": 0
            }

        df = self.make_request(self.device_data_url,
                               filter_, 'Packets', silent=silent)
        df.index = pd.to_datetime(
            df.SendDate, dayfirst=True) + datetime.timedelta(hours=utc_hour_dif)
        df.index.name = 'Date'
        df.sort_index(axis=0, level=None, ascending=True, inplace=True)
        df.columns = [self.right_cols[col.lower()] if col.lower(
        ) in self.right_cols else col for col in df.columns]
        return df.dropna(axis=1, how='all')

    def get_devices_data(self, *serial_numbers, start_date=None,
                         finish_date=None, param='PM2.5', utc_hour_dif=7, max_packets_count=10000, silent=False):
        df = pd.DataFrame()
        for serial_number in serial_numbers:
            try:
                new_series = self.get_device_data(serial_number, start_date=start_date, finish_date=finish_date,
                                                  utc_hour_dif=utc_hour_dif,
                                                  max_packets_count=max_packets_count, silent=silent)[param].resample(
                    '1T').mean()
            except Exception as e:
                print(e.__str__())
                new_series = pd.Series()
            new_series.name = serial_number
            df = pd.concat([df, new_series], axis=1)
        return df

    def get_stations(self, full_info=False, silent=False):
        filter_ = {}
        df = self.make_request(self.stations_url, filter_,
                               'MonitoringStations', silent=silent)
        df.index = df.MonitoringObjectId
        if full_info:
            return df
        else:
            return df['Name']

    def get_station_data(self, station_id,
                         start_date=None,
                         finish_date=datetime.datetime.now(), period='5min', silent=False):
        time_periods = {'5min': 1, '20min': 2, '1hr': 3, '24hr': 4}
        if start_date:
            finish_date = self.to_date(finish_date)
            start_date = self.to_date(start_date)
            filter_ = {
                "MonitoringObjectId": f"{station_id}",
                "FilterType": 1,
                "IntervalType": time_periods[period],
                "BeginTime": start_date.isoformat(),
                "EndTime": finish_date.isoformat()}
        else:
            filter_ = {
                "MonitoringObjectId": f"{station_id}",
                "FilterType": 3,
                "IntervalType": 1,
                "SkipFromLast": 0,
                "TakeCount": 2016
            }
        tmp_df = self.make_request(
            self.station_data_url, filter_, 'PacketItems', silent=silent)
        if len(tmp_df.index) == 0:
            return pd.DataFrame(columns=['PM2.5', 'PM10', 'T', 'RH', 'P'])
        df = pd.DataFrame()
        for data_str in tmp_df['DataJson']:
            df = df.append(dict([(param['Id'], param['Sum'] / param['Cnt']) for param in json.loads(data_str)]),
                           ignore_index=True)
        df.index = pd.to_datetime(tmp_df['SendDate'], dayfirst=True)
        df.index.name = 'Date'
        #  переименовываю колонки, названия из запроса для станций
        d = dict([(param['PacketParamLinkId'], param['ParamName']) for param in
                  self.get_stations(full_info=True, silent=True)['PacketParamLinkItems'][station_id]])
        df.columns = [d[param_id] for param_id in df.columns]
        # переименоываю, чтоб красивее было
        df.columns = [self.right_cols[col.lower()] if col.lower(
        ) in self.right_cols else col for col in df.columns]
        df.sort_index(axis=0, level=None, ascending=True, inplace=True)
        return df

    def get_stations_data(self, *station_ids,
                          start_date=None,
                          finish_date=datetime.datetime.now(), period='5min', param='PM2.5', silent=False):
        df = pd.DataFrame()
        for station_id in station_ids:
            try:
                new_series = \
                    self.get_station_data(
                        station_id, start_date, finish_date, period, silent=silent)[param]
            except Exception:
                new_series = pd.Series()
            new_series.name = station_id
            df = pd.concat([df, new_series], axis=1)
        df.columns = station_ids
        mo_names = self.get_stations(silent=True)
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
