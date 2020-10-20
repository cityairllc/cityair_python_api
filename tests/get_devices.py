import pandas as pd


def test_device_list(R):
    serials = R.get_devices()
    assert isinstance(serials, list)


def test_device_list_without_children(device_list):
    for serial_number in device_list:
        assert serial_number.startswith("CA")


def test_device_list_with_children(device_list_with_children):
    prefixes = ["G1", "G2", "24"]
    for prefix in prefixes:
        assert any(filter(lambda serial: serial.startswith(prefix),
                          device_list_with_children)
                   )


def test_devices_df(R):
    df = R.get_devices(format='df')
    assert isinstance(df, pd.DataFrame)
