import json

import requests


def anonymize_request(body: dict) -> dict:
    request = body.copy()
    request.update(Token="***")
    return request


class CityAirException(Exception):
    pass


class NoAccessException(CityAirException):
    """
    raised when the requested device is not assigned to the user
    """

    def __init__(self, serial_number: str):
        message = (f"Sorry, you don't have access to the device "
                   f"{serial_number}. Maybe you're trying to access "
                   f"child device (i.e. G1, G2, etc), you should "
                   f"request data from the main device")
        super().__init__(message)


class TransportException(CityAirException):
    """
    raised when request contains bad json
    """

    def __init__(self, response: requests.models.Response):
        body = anonymize_request(
                json.loads(response.request.body.decode('utf-8')))
        message = (f"Error while getting data:\n"
                   f"url: {response.url}\n"
                   f"request body: {json.dumps(body)}\n"
                   f"request headers: {response.headers}\n"
                   f"response code: {response.status_code}"
                   f"response headers: {response.headers}"
                   f"response content: {response.content}")
        super().__init__(message)


class ServerException(CityAirException):
    """
    cityair backend exception. raised when request contains 'IsError'=True
    """

    def __init__(self, response: requests.models.Response):
        body = anonymize_request(
                json.loads(response.request.body.decode('utf-8')))
        message = (f"Error while getting data:\n"
                   f"url: {response.url}\n"
                   f"request body: {json.dumps(body)}\n")
        try:
            message += (f"{response.json()['ErrorMessage']}:\n"
                        f"{response.json().get('ErrorMessageDetals')}")
        except KeyError:
            message += str(response.json())
        super().__init__(message)


class EmptyDataException(CityAirException):
    """
    raised whe 'Result' field in response is empty
    """

    def __init__(self, response: requests.models.Response = None, item=None):
        message = ("No data for the request. Try changing query arguments, "
                   "i.e. start_date or finish_date.\n")
        if response:
            body = anonymize_request(
                    json.loads(response.request.body.decode('utf-8')))
            message += (f"url: {response.url}\n"
                        f"request body: {json.dumps(body)}\n")
        if item:
            message = message.replace("request", f"{item}")
        super().__init__(message)
