Inroduction
^^^^^^^^^^^^^^^^^^^^^
python package for getting CityAir.io data in a format of Pandas Dataframes for further proccessing.
enjoy and feel free to leave a feedback at feedback@cityair.ru


Installing
^^^^^^^^^^^^^^^^^^^^^
the last available version is 0.1.1. You can install or upgrade the stable cityair-api version with: ::

    $ pip install cityair-api -U

Example
^^^^^^^^^^^^^^^^^^^^^
first you need to init the CityAirRequest object passing your login and password: ::

     from cityair_api import CityAirRequest

     r = CityAirRequest('CityAir_demo', 'cityAirDemoPassword231')

Getting data from stations
****************************
We highly recommend to use stations API. First you may want to get the stations list available to you: ::

    stations = r.get_stations()

then you can get the data collected by the stations. Important arguments are:

    - start_date, finish_date - may be in datetime or string formats
    - period is time resolution and may be '5min', '20min', '1hr','24hr' 


::

    df = r.get_station_data(stations.iloc[0])

... or by multiple stations: ::

   df = r.get_stations_data(stations, param = 'PM2,5')
  
Getting data directly from devices
******************************************
Using this API you get raw data directly from measuring devices. This API is rather slow dealing with big datasets, so be prepeared :)

The main steps are the same as with stations API: ::

    devices = r.get_devices()    

afterward get data from single or multiple sources: ::

    df = r.get_device_data(device_serial_number)
    df = r.get_devices_data(devices, param='PM2.5')


