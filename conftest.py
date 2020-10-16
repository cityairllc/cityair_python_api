import random

import pytest

from cityair_api import CAR


@pytest.fixture(scope="session")
def R():
    return CAR()


@pytest.fixture(scope="session")
def online_serial_number(R):
    serial_number = random.choice(R.get_devices(include_offline=False))
    return serial_number


@pytest.fixture(scope="session")
def random_serial_number(R):
    serial_number = random.choice(R.get_devices())
    return serial_number


@pytest.fixture(scope="session")
def online_station_id(R):
    station_id = random.choice(R.get_stations(include_offline=False))
    return station_id
