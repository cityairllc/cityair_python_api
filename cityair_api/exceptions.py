import requests

class CityAirException(Exception):
    pass


class NoAccessException(CityAirException):
    pass


class ServerException(CityAirException):
    def __init__(self, response: requests.models.Response):
        message = f"Error while getting data:\nurl: {response.url}\nrequest body: {response.request.body}\n{response.json()['ErrorMessage']}:\n{response.json()['ErrorMessageDetals']}"
        super().__init__(message)


class EmptyDataException(CityAirException):
    def __init__(self, response: requests.models.Response):
        message = f"No data for the request. Try changing request arguements:\nurl: {response.url}\nrequest body: {response.request.body}\n"
        super().__init__(message)


class CityAirRequest:
    def __init__(self, user, psw, **kwargs):
        self.host_url = kwargs.get('host_url', DEFAULT_HOST)
        self.timeout = kwargs.get('timeout', 100)
        self.user = user
        self.psw = psw