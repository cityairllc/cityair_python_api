import random
import pandas as pd

from api_test_case import CityAirApiTestCase
from cityair_api import CAR


class TestMainMethods(CityAirApiTestCase):
    def setup(self):
        self.serial_number = random.choice(self.r.get_devices())
        self.station_id = random.choice(self.r.get_stations(include_offline=False))


    def test_get_devices(self):
        devices = self.r.get_devices()
        assert isinstance(devices, list)

    def test_get_device_data(self):
        df = self.r.get_device_data(self.serial_number, take_count=5)
        assert isinstance(df, pd.DataFrame)

    def test_get_stations(self):
        stations = self.r.get_stations()
        assert isinstance(stations, list)

    def test_get_station_data(self):
        df = self.r.get_station_data(self.station_id)
        assert isinstance(df, pd.DataFrame)

    def test_get_locations(self):
        locations = self.r.get_locations()
        assert isinstance(locations, list)
