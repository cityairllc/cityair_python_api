import random
from datetime import datetime, timedelta

from api_test_case import CityAirApiTestCase


class TestOnlineStationData(CityAirApiTestCase):

    def setup(self):
        self.station_id = random.choice(
                self.r.get_stations(include_offline=False))
        self.finish_date = datetime.now()
        self.start_date = self.finish_date - timedelta(days=1)

    def test_last_day(self):
        df = self.r.get_station_data(self.station_id,
                                     start_date=self.start_date,
                                     finish_date=self.finish_date)
        assert not df.empty
