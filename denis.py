from cityair_api import CityAirRequest


import logging
logging.basicConfig(level=logging.DEBUG)
serial = 'CA01PMDB5F56 ICCID'
R = CityAirRequest()

print(help( R.get_logs ))

extract_pattern = r"SourceDataString=\[([^']*)\]"
filter_pattern = r"\[[^']*"
for p in R.get_logs(serial, take_count = 1000, type = 'custom', app_sender_ids=[],
                   extract_pattern=extract_pattern, filter_pattern = filter_pattern,
                   to_include_date = True):
    print(p)