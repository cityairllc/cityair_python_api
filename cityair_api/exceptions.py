import requests
import json


"""
TODO

add docstrings

"""
class CityAirException(Exception):
    pass


class NoAccessException(CityAirException):
    def __init__(self, serial_number: str):
        message = f"Sorry, you don't have access to the device {serial_number}. Maybe you're trying to access child device (i.e. G1, G2, etc), you should request data from the main device"
        super().__init__(message)


class ServerException(CityAirException):
    def __init__(self, response: requests.models.Response):
        body = json.loads(response.request.body.decode('utf-8'))
        body.update(User='***', Pwd='***')
        message = f"Error while getting data:\nurl: {response.url}\nrequest body: {body}\n"
        try:
            message+= f"{response.json()['ErrorMessage']}:\n{response.json()['ErrorMessageDetals']}"
        except KeyError:
            message += str(response.json())
        super().__init__(message)


class EmptyDataException(CityAirException):
    def __init__(self, response: requests.models.Response = None, serial_number: str = None):
        message = f"No data for the request. Try changing query arguments, i.e. start_date or finish_date."
        if response:
            body = json.loads(response.request.body.decode('utf-8'))
            body.update(User='***', Pwd='***')
            message += "\nurl: {response.url}\nrequest body: {body}\n"
        if serial_number:
            message = message.replace("request", f"{serial_number} device")
        super().__init__(message)