

from cityair_api.request import CityAirRequest



r = CityAirRequest('ek', 'Oracle23')
serial_number  = 'CA01PM0000FF'
df = r.get_device_lastweek_data(serial_number)


