import pandas as pd


def test_device_list(R):
    serials = R.get_devices()
    assert isinstance(serials, list)


def test_device_list(R):
    for serial_number in R.get_devices():
        assert serial_number.startswith("CA")


def test_devices_df(R):
    df = R.get_devices(format='df')
    assert isinstance(df, pd.DataFrame)
