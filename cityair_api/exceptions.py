import json

import requests




def anonymize_request(body: dict) -> dict:
    res = body.copy()
    res.update(User='***', Pwd='***')
    return res


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


class ServerException(CityAirException):
    """
    unknown cityair backend exception
    raised when request contains 'IsError'=True
    """

    def __init__(self, response: requests.models.Response):
        body = json.loads(response.request.body.decode('utf-8'))
        body.update(User='***', Pwd='***')
        message = (f"Error while getting data:\nurl: "
                   f"{response.url}\nrequest body: {body}\n")
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
        message = (f"No data for the request. Try changing query arguments, "
                   f"i.e. start_date or finish_date.")
        if response:
            body = anonymize_request(
                json.loads(response.request.body.decode('utf-8')))
            message += f"\nurl: {response.url}\nrequest body: {body}\n"
        if item:
            message = message.replace("request", f"{item}")
        super().__init__(message)
