from cityair_api import CityAirRequest
import pandas as pd

TAKE_COUNT = 5
START_DATE = '19.10.2019'
FINISH_DATE = '21.10.2019'

r = CityAirRequest('ek', 'Oracle23')

devices = r.get_devices(include_children=True)
print('\n\n----------------------devices as list-------------------')
print(devices[:2])
assert isinstance(devices[0], str), f'Serial number should be string got {type(devices[0])} instead'

devices_df = r.get_devices(format='df', include_children=True)
print('\n\n----------------------devices as df-------------------')
print(devices_df.tail(2))
assert isinstance(devices_df.index[0], str)

devices_dicts = r.get_devices(format='dicts', include_children=True)
print('\n\n------------------devices as dicts----------------------')
print(devices_dicts[-2:])

serial_number = 'CA01PM0000F8'
print(f'\n\n----------------getting {serial_number} device data as one df-----------------------')
df = r.get_device_data(serial_number,
                       start_date=START_DATE,
                       finish_date=FINISH_DATE,
                       take_count=300,
                       format='df')
print(df.head(2))
assert isinstance(df.index, pd.core.indexes.datetimes.DatetimeIndex), f'the type of df.index is {type(df.index)}'

serial_number = 'CA01PM000105'
print(f'\n\n----------------getting {serial_number} device data as dict-----------------------')
dict_ = r.get_device_data(serial_number,
                          start_date=START_DATE,
                          finish_date=FINISH_DATE,
                          take_count=300,
                          format='dict')
for key in dict_:
    print(f"{key}:\n{dict_[key].head(2)}")
    assert isinstance(dict_[key].index, pd.core.indexes.datetimes.DatetimeIndex), f'the type of df.index is {type(df.index)}'

print(f'\n\n----------------getting stations as list-----------------------')
station_ids = r.get_stations()
print(f"first station_ids: {station_ids[:5]}")

print(f'\n\n----------------getting stations as dicts-----------------------')
for station_info in r.get_stations(format='dicts')[:2]:
    print(station_info)

station_id = station_ids[-10]
print(f'\n\n----------------getting station {station_id} data-----------------------')
df = r.get_station_data(station_id,
                        start_date=START_DATE)
print(df.head(2))

station_id = station_ids[-10]
print(f'\n\n----------------getting station {station_id} data in debug mode-----------------------')
df = r.get_station_data(station_id,
                        start_date=START_DATE,
                        take_count=2,
                        debug=True)
print(df.head())


print(f'\n\n----------------getting locations-----------------------')
locations = r.get_locations()
print(locations[:2])
