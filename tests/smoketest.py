import pandas as pd


def test_get_devices(R):
    devices = R.get_devices()
    assert isinstance(devices, list)


def test_get_device_data(random_serial_number, R):
    df = R.get_device_data(random_serial_number, take_count=5)
    assert isinstance(df, pd.DataFrame)


def test_get_stations(R):
    stations = R.get_stations()
    assert isinstance(stations, list)


def test_get_station_data(online_station_id, R):
    df = R.get_station_data(online_station_id)
    assert isinstance(df, pd.DataFrame)


def test_get_locations(R):
    locations = R.get_locations()
    assert isinstance(locations, list)
