import random
from datetime import datetime, timedelta

from api_test_case import CityAirApiTestCase
from pytz import utc


class TestOnlineDeviceData(CityAirApiTestCase):

    def setup(self):
        self.serial = random.choice(self.r.get_devices(include_offline=False))

    def test_last_packet(self):
        df = self.r.get_device_data(self.serial, take_count=5)
        now = datetime.utcnow()
        last_packet_date = df.index[-1]
        assert now - last_packet_date < timedelta(hours=1)

    def test_first_packet(self):
        df = self.r.get_device_data(self.serial, take_count=5,
                                    last_packet_id=0, all_cols=True)
        first_packet_id = df.iloc[0]['packet_id']
        assert first_packet_id == 1
