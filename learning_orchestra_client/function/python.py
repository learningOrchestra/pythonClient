from learning_orchestra_client.observe.observe import Observer
from learning_orchestra_client._util._response_treat import ResponseTreat
from learning_orchestra_client._util._entity_reader import EntityReader
import requests
from typing import Union


class FunctionPython:
    __CODE_FIELD = "function"
    __PARAMETERS_FIELD = "functionParameters"
    __NAME_FIELD = "name"
    __DESCRIPTION_FIELD = "description"

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/function/python"
        self.__service_url = f'{cluster_ip}{self.__api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def run_function_sync(self,
                          name: str,
                          parameters: dict,
                          code: str,
                          description: str = "",
                          pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method runs a python 3 code in sync mode, so it
        represents a wildcard for the data scientist. It can be used when
        train, predict, tune, explore or any other pipe must be customized. The
        function is also useful for new pipes. pretty_response: If true it
        returns a string, otherwise a dictionary.

        name: Is the name of the object stored in Learning Orchestra storage
        system (volume or mongoDB).
        url: Url to CSV file.

        return: A JSON object with an error or warning message or the correct
        operation result.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__PARAMETERS_FIELD: parameters,
            self.__CODE_FIELD: code,
            self.__DESCRIPTION_FIELD: description}

        request_url = self.__service_url
        response = requests.post(url=request_url, json=request_body)
        self.__observer.wait(name)

        return self.__response_treat.treatment(response, pretty_response)

    def run_function_async(self,
                           name: str,
                           parameters: dict,
                           code: str,
                           description: str = "",
                           pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method runs a python 3 code in async mode, so it
        represents a wildcard for the data scientist. It does not lock the
        caller, so a wait method must be used. It can be used when train,
        predict, tune, explore or any other pipe must be customized. The
        function is also useful for new pipes.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the function to be called
        code: the Python code
        parameters: the parameters of the function being called

        return: A JSON object with an error or warning message or the correct
        operation result.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__PARAMETERS_FIELD: parameters,
            self.__CODE_FIELD: code,
            self.__DESCRIPTION_FIELD: description}

        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        return self.__response_treat.treatment(response, pretty_response)

    def search_all_executions(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all created functions metadata,
        i.e., it does not retrieve the function result content.

        pretty_response: If true it returns a string, otherwise a dictionary.

        return: All function executions metadata stored in Learning Orchestra
        or an empty result.
        """
        response = self.__entity_reader.read_all_instances_from_entity()
        return self.__response_treat.treatment(response, pretty_response)

    def delete_execution(self, name: str, pretty_response=False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting the function.
        This delete operation is asynchronous, so it does not lock the caller
         until the deletion finished. Instead, it returns a JSON object with a
         URL for a future use. The caller uses the URL for delete checks.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Represents the function name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        request_url = f'{self.__service_url}/{name}'

        response = requests.delete(request_url)
        return self.__response_treat.treatment(response, pretty_response)

    def search_execution_content(self,
                                 name: str,
                                 query: dict = {},
                                 limit: int = 10,
                                 skip: int = 0,
                                 pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving the function
        results, including metadata. A function is executed many times, using
        different parameters,
        thus many results are stored
        in Learning Orchestra.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the function.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return:
         A page with some function results inside or an error if there
        is no such function. The current page is also returned to be used in
        future content requests.
        """

        response = self.__entity_reader.read_entity_content(
            name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, dataset_name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the run_function_async method or delete_function method.

           name: Represents the function name.
           timeout: Represents the time in seconds to wait for a function to
           finish its run.

           return: JSON object with an error message, a warning message or a
           correct function result
        """
        return self.__observer.wait(dataset_name, timeout)
