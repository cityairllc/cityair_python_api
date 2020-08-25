from cityair_api import CAR


class CityAirApiTestCase:

    def setup_class(self):
        self.r = CAR(verify_ssl=False)
