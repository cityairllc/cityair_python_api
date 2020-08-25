import pandas as pd

from api_test_case import CityAirApiTestCase


class TestGetDevices(CityAirApiTestCase):

    def test_device_list(self):
        serials = self.r.get_devices()
        assert isinstance(serials, list)

    def test_devices_df(self):
        df = self.r.get_devices(format='df')
        assert isinstance(df, pd.DataFrame)
