from datetime import datetime, timedelta


def test_last_packet(R, online_serial_number):
    df = R.get_device_data(online_serial_number, take_count=5)
    now = datetime.utcnow()
    last_packet_date = max(df.index)
    assert now - last_packet_date < timedelta(hours=1)


def test_first_packet(random_serial_number, R):
    df = R.get_device_data(random_serial_number, take_count=5,
                           last_packet_id=0, all_cols=True)
    first_packet_id = df.iloc[0]['packet_id']
    assert first_packet_id == 1
