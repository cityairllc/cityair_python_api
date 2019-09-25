import requests

class ServerException(Exception):
    def __init__(self, response: requests.models.Response):
        message = f"Error while getting data:\nurl: {response.url}\nrequest body: {response.request.body}\n{response.json()['ErrorMessage']}:\n{response.json()['ErrorMessageDetals']}"
        super().__init__(message)


class EmptyDataException(Exception):
    def __init__(self, response: requests.models.Response):
        message = f"No data for the request. Try changing request arguements:\nurl: {response.url}\nrequest body: {response.request.body}\n"
        super().__init__(message)