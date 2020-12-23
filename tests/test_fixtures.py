def test_devices(device_list):
    for serial_number in device_list:
        assert serial_number.startswith("CA")


def test_online_devices(online_device_list, device_list):
    print(1)
    assert len(device_list) > len(online_device_list)

    for serial_number in online_device_list:
        assert serial_number.startswith("CA")
