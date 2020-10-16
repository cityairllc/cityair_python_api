from datetime import datetime, timedelta


def test_last_day(online_station_id, R):
    finish = datetime.now()
    start = finish - timedelta(days=1)

    df = R.get_station_data(online_station_id, start_date=start,
                            finish_date=finish)
    assert not df.empty
