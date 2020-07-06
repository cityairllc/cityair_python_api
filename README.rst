Inroduction
^^^^^^^^^^^^^^^^^^^^^
python package for getting CityAir.io data in the format of Pandas DataFrames for further processing.
enjoy and feel free to leave a feedback at feedback@cityair.ru


Installing
^^^^^^^^^^^^^^^^^^^^^
You can install the latest version with: ::

    $ pip install cityair-api -U

Example
^^^^^^^^^^^^^^^^^^^^^
first you need to init the CityAirRequest object passing your login and password: ::

     from cityair_api import CityAirRequest
     CITYAIR_TOKEN = "0695b654-b9cf-47c8-a21e-ab9dc2d95fd3"
     r = CityAirRequest(CITYAIR_TOKEN)

if CITYAIR_TOKEN is not passed, r will try to get it from environmental variables, then it will prompt for token

you could get CITYAIR_TOKEN from your personal page at cityair.io or you could look up for it in the web-browser dev tools while you're logging in

Getting data from stations
****************************
We highly recommend to use stations API. First you may want to get the stations list available to you: ::

    stations = r.get_stations()

then you can get the data collected by the stations. Important arguments are:

    - start_date, finish_date - may be in datetime or string formats. if not passed - the last packets are fetched if quantity of packets_count
    - period is time resolution and may be 5 mins, 20 min, 1 hour or day - you should pass Period.ENUM


::

    df = r.get_station_data(stations[0], Period.HOUR)


Getting data directly from devices
******************************************
Using this API you get raw data directly from measuring devices. This API is rather slow dealing with large datasets, so be prepeared :)

The main steps are the same as with stations API: ::

    serial_numbers = r.get_devices()

afterwards get dataset: ::

    df = r.get_device_data(serial_numbers[0])

TODO
******

* tests
    * required fields with (all_cols=True/False)
    * getting data with different settings
    * get_data with different start/finish - unaware, None, str
* refactor
    * move settings to separate file
    * refactor useless_cols mess
