from .._response_treat import ResponseTreat
from .._entity_reader import EntityReader
from ..observer import Observer
import requests
from typing import Union


class TransformDataType:
    __INPUT_NAME = "inputDatasetName"
    __TYPES = "types"

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/transform/dataType"
        self.__service_url = f'{cluster_ip}{self.__api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def update_dataset_type_sync(self,
                             dataset_name: str,
                             types: dict,
                             pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: Change types of fields to number or string.

        dataset_name: Represents the dataset name.
        fields_change: Fields to change with types. This is a dict with each
        key:value being field:type

        return: A JSON object with error or warning messages.
        """

        url_request = self.__service_url
        body_request = {
            self.__INPUT_NAME: dataset_name,
            self.__TYPES: types
        }

        response = requests.patch(url=url_request, json=body_request)
        self.__observer.wait(dataset_name)

        return self.__response_treat.treatment(response, pretty_response)

    def update_dataset_type_async(self,
                             dataset_name: str,
                             types: dict,
                             pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: Change types of fields to number or string.

        dataset_name: Represents the dataset name.
        fields_change: Fields to change with types. This is a dict with each
        key:value being field:type

        return: A JSON object with error or warning messages.
        """

        url_request = self.__service_url
        body_request = {
            self.__INPUT_NAME: dataset_name,
            self.__TYPES: types
        }

        response = requests.patch(url=url_request, json=body_request)
        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, dataset_name: str) -> dict:
        return self.__observer.wait(dataset_name)