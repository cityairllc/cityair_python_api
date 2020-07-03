from cityair_api import CityAirRequest

login = 'CityAir_demo'
psw = 'cityAirDemoPassword231'

r = CityAirRequest(login, psw)

# it's better to use stations, because this API is more optimised.
# first you need to get all stations you have access to
stations = r.get_stations()

# then, after you know station_id, make a request for data
# important params are start_date, finish_date and period
# dates may be in datetime or string formats
# period is time resolution and may be '5min', '20min', '1hr','24hr'
station_id = stations[0]
df_station = r.get_station_data(station_id, start_date='2018.12.01',
                                finish_date='2018.12.01')

# also you can get access to data directly from the device
serials = r.get_devices()
serial = serials[0]
df_device = r.get_device_data(serial)
