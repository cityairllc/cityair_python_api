from cityair_python_api.cityair_requests import CityAirRequest
from cityair_python_api.cityair_graphs import *

login = 'CityairDemo'
psw = 'demoAccPsw'

r = CityAirRequest(login, psw)

# it's better to use stations, because this API is more optimised.
# first you need to get all stations you have access to
stations = r.get_stations()

# then, after you know station_id, make a request for data
# important params are start_date, finish_date and period
# dates may be in datetime or string formats
# period is time resolution and may be '5min', '20min', '1hr','24hr'
station_id = list(stations.keys())[0]
df_station = r.get_station_data(station_id, start_date='2018.12.01', finish_date='2018.12.01')
# moreover you can get data from several station, if you're interested in comparing data
df_stations = r.get_stations_data(stations.keys(), param='PM2.5')

# also you can get access to data directly from the device
devices = r.get_devices()
device_serial_number = devices[0]
df_device = r.get_device_data(device_serial_number)
df_devices = r.get_devices_data(devices, param='PM2.5')

# all graphs are to be used inside jupyter notebooks,
# although you can save graph_time as html file if you provide descr as argument
graph_time(df_devices, descr="out")
