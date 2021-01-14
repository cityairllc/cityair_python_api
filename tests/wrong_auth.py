import pytest

from cityair_api import CityAirException


@pytest.fixture
def R_with_wrong_token(R):
    R.token = R.token[:-1] + "1"
    return R


def test_wrong_token(R_with_wrong_token):
    with pytest.raises(CityAirException, match=r".*Token=.*is not found.*"):
        R_with_wrong_token.get_devices()


@pytest.fixture
def R_with_invalid_token(R):
    R.token = "321"
    return R


def test_invalid_token(R_with_invalid_token):
    with pytest.raises(CityAirException,
                       match=r".*Guid should contain 32 digits with 4 "
                             r"dashes.*"):
        R_with_invalid_token.get_devices()
