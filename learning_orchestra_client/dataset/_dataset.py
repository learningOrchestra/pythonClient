from ..observer import Observer
from .._response_treat import ResponseTreat
from .._entity_reader import EntityReader
import requests
from typing import Union


class Dataset:
    __DATASET_NAME = "datasetName"
    __URL = "datasetURI"

    def __init__(self, cluster_ip: str, api_path: str):
        self.__service_url = f'{cluster_ip}{api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def insert_dataset_sync(self,
                            dataset_name: str,
                            url: str,
                            pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method is responsible to insert a dataset from a URI
        synchronously, i.e., the caller waits until the dataset is inserted into
        the Learning Orchestra storage mechanism.

        pretty_response: If true return indented string, else return dict.
        dataset_name: Is the name of the dataset file that will be created.
        url: Url to CSV file.

        return: A JSON object with an error or warning message or a URL
        indicating the correct operation.
        """
        request_body = {self.__DATASET_NAME: dataset_name,
                        self.__URL: url}
        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        self.__observer.wait(dataset_name)

        return self.__response_treat.treatment(response, pretty_response)

    def insert_dataset_async(self,
                             dataset_name: str,
                             url: str,
                             pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible to insert a dataset from a URI
        asynchronously, i.e., the caller does not wait until the dataset is
        inserted into the Learning Orchestra storage mechanism. Instead, the
        caller receives a JSON object with a URL to proceed future calls to
        verify if the dataset is inserted.

        pretty_response: If true return indented string, else return dict.
        dataset_name: Is the name of the dataset file that will be created.
        url: Url to CSV file.

        return: A JSON object with an error or warning message or a URL
        indicating the correct operation (the caller must use such an URL to
        proceed future checks to verify if the dataset is inserted).
        """
        request_body = {self.__DATASET_NAME: dataset_name,
                        self.__URL: url}
        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        return self.__response_treat.treatment(response, pretty_response)

    def search_all_datasets(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all datasets metadata, i.e., it does
        not retrieve the dataset content.

        pretty_response: If true return indented string, else return dict.

        return: All datasets metadata stored in Learning Orchestra or an empty
        result.
        """
        response = self.__entity_reader.read_all_instances_from_entity()
        return self.__response_treat.treatment(response, pretty_response)

    def delete_dataset_async(self, dataset_name, pretty_response=False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting the dataset.
        This delete operation is asynchronous, so it does not lock the caller
         until the deletion finished. Instead, it returns a JSON object with a
         URL for a future use. The caller uses the URL for delete checks. If a
         dataset was used by another task (Ex. projection, histogram, pca, tune
         and so forth), it cannot be deleted.

        pretty_response: If true return indented string, else return dict.
        dataset_name: Represents the dataset name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        request_url = f'{self.__service_url}/{dataset_name}'
        response = requests.delete(request_url)

        return self.__response_treat.treatment(response, pretty_response)

    def search_dataset_content(self,
                               dataset_name: str,
                               query: dict = {},
                               limit: int = 10,
                               skip: int = 0,
                               pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving the dataset
        content

        pretty_response: If true return indented string, else return dict.
        dataset_name: Is the name of the dataset file.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return A page with some tuples or registers inside or an error if there
        is no such dataset. The current page is also returned to be used in
        future content requests.
        """

        response = self.__entity_reader.read_entity_content(
            dataset_name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, dataset_name: str) -> dict:
        return self.__observer.wait(dataset_name)
