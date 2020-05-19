TOKEN_VAR_NAME = 'CITYAIR_TOKEN'

DEFAULT_HOST = "https://my.cityair.io/api/request.php?map="
DEVICES_URL = "DevicesApi2/GetDevices"
DEVICES_PACKETS_URL = "DevicesApi2/GetPackets"
STATIONS_URL = "MoApi2/GetMoItems"
STATIONS_PACKETS_URL = "MoApi2/GetMoPackets"
LOGS_URL = "/LoggerApi/GetLogItems"
FULL_LOGS_URL ="/LoggerApi/GetFullLogItems"

PACKET_SENDER_IDS = [{"AppId": 4, "SenderIds": [23]}, {"AppId": 2, "SenderIds": [7]}]  # for logs lookups

LOG_CHECKINFO_FILTER_PATTERN = "#CheckInfo#{[^']*}"
LOG_CHECKINFO_ADDITIONAL_FILTER_SUFFIX = " CheckInfo"
LOG_PACKET_FILTER_PATTERN = r"#PT#\d+"
LOG_PACKET_ADDITIONAL_FILTER_SUFFIX = " PT"
LOG_EXTRACT_PATTERN = r"#([^']*)##"
CHECKINFO_PARSE_PATTERN = r"#CheckInfo#{([^'\n]*)}\n"