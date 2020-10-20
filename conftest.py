import random

import pytest

from cityair_api import CAR


@pytest.fixture(scope="session")
def R():
    return CAR()


@pytest.fixture(scope="session")
def device_list(R):
    return R.get_devices()


@pytest.fixture(scope="session")
def online_device_list(R):
    return R.get_devices(include_offline=True)


@pytest.fixture(scope="session")
def device_list_with_children(R):
    return R.get_devices(include_children=True)


@pytest.fixture()
def online_serial_number(online_device_list):
    return random.choice(online_device_list)


@pytest.fixture()
def random_serial_number(device_list):
    return random.choice(device_list)


@pytest.fixture(scope="session")
def online_station_id(R):
    station_id = random.choice(R.get_stations(include_offline=False))
    return station_id
