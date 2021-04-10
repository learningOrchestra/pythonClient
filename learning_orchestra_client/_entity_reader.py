from ._response_treat import ResponseTreat
import requests
from requests import Response


class EntityReader:
    def __init__(self, entity_url: str):
        self.__response_treat = ResponseTreat()
        self.__entity_url = entity_url

    def read_all_instances_from_entity(self) -> Response:
        request_url = self.__entity_url

        response = requests.get(request_url)
        return response

    def read_entity_content(self,
                            name: str,
                            query: dict = {},
                            limit: int = 10,
                            skip: int = 0) \
            -> Response:
        request_url = f'{self.__entity_url}/{name}' \
                      f'?query={query}&limit={limit}&skip={skip}'

        response = requests.get(request_url)
        return response