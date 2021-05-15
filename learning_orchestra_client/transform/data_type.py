from learning_orchestra_client._util._response_treat import ResponseTreat
from learning_orchestra_client._util._entity_reader import EntityReader
from learning_orchestra_client.observe.observe import Observer
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
        description: Change dataset field types (from number to string and
        vice-versa). Many type modifications can be performed in one method
        call.

        dataset_name: Represents the dataset name.
        types: Represents a map, where the pair key:value is a field:type

        return: A JSON object with error or warning messages or a correct
        datatype result.
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
        description: Change dataset field types (from number to string and
        vice-versa). Many type modifications can be performed in one method
        call. Is is an asynchronous call, thus a wait method must be also
        called to guarantee a synchronization barrier.

        dataset_name: Represents the dataset name.
        types: Represents a map, where the pair key:value is a field:type

        return: A JSON object with error or warning messages or a correct
        datatype result.
        """
        url_request = self.__service_url
        body_request = {
            self.__INPUT_NAME: dataset_name,
            self.__TYPES: types
        }

        response = requests.patch(url=url_request, json=body_request)
        return self.__response_treat.treatment(response, pretty_response)

    def search_all_datatype(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all datatype metadata, i.e., it does
        not retrieve the datatype content.

        pretty_response: If true it returns a string, otherwise a dictionary.

        return: All predict metadata stored in Learning Orchestra or an empty
        result.
        """
        response = self.__entity_reader.read_all_instances_from_entity()
        return self.__response_treat.treatment(response, pretty_response)

    def delete_datatype(self, name: str, pretty_response=False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting the datatype step.
        This delete operation is asynchronous, so it does not lock the caller
         until the deletion finished. Instead, it returns a JSON object with a
         URL for a future use. The caller uses the URL for delete checks.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Represents the datatype name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """
        request_url = f'{self.__service_url}/{name}'

        response = requests.delete(request_url)
        return self.__response_treat.treatment(response, pretty_response)

    def search_datatype_content(self,
                                name: str,
                                query: dict = {},
                                limit: int = 10,
                                skip: int = 0,
                                pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving all the datatype
        tuples or registers, as well as the metadata content

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the datatype object
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: A page with some registers or tuples inside or an error if there
        is no such datatype object. The current page is also returned to be
        used in future content requests.
        """
        response = self.__entity_reader.read_entity_content(
            name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, dataset_name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the update_dataset_type_async method, delete_datatype
           method.

           name: Represents the datatype name.
           timeout: Represents the time in seconds to wait for a datatype to
           finish its run.

           return: JSON object with an error message, a warning message or a
           correct datatype result
        """
        return self.__observer.wait(dataset_name, timeout)
