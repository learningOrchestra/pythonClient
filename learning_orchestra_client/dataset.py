__author__ = "Otavio Henrique Rodrigues Mapa & Matheus Goncalves Ribeiro"
__credits__ = "all free source developers"
__status__ = "Prototype"

from .observer import Observer
from ._response_treat import ResponseTreat
import requests
from typing import Union


class Dataset:
    def __init__(self, ip_from_cluster: str):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/dataset"
        self.response_treat = ResponseTreat()
        self.OUTPUT_NAME = "datasetName"
        self.URL = "datasetURI"
        self.CLUSTER_IP = ip_from_cluster

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
        request_body = {self.OUTPUT_NAME: dataset_name,
                        self.URL: url}
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        Observer(dataset_name, self.CLUSTER_IP).observe_processing()

        if pretty_response:
            print("\n----------" + " CREATED FILE " + dataset_name + " -------"
                                                                     "---")
        return self.response_treat.treatment(response, pretty_response)

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
        request_body = {self.OUTPUT_NAME: dataset_name,
                        self.URL: url}
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        if pretty_response:
            print("\n----------" + " CREATED FILE " + dataset_name + " -------"
                                                                     "---")
        return self.response_treat.treatment(response, pretty_response)

    def search_all_datasets(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all datasets metadata, i.e., it does
        not retrieve the dataset content.

        pretty_response: If true return indented string, else return dict.

        return: All datasets metadata stored in Learning Orchestra or an empty
        result.
        """
        request_url = self.cluster_url

        response = requests.get(request_url)

        return self.response_treat.treatment(response, pretty_response)

    def search_dataset(self, dataset_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for retrieving a specific
        dataset

        pretty_response: If true return indented string, else return dict.
        dataset_name: Is the name of the dataset file.
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return Specific dataset metadata stored in Learning Orchestra or an
        error if there is no such dataset.
        """
        response = self.search_dataset_content(dataset_name, limit=1,
                                               pretty_response=pretty_response)

        return response

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

        request_url = self.cluster_url + "/" + dataset_name + \
                      "?query=" + str(query) + \
                      "&limit=" + str(limit) + \
                      "&skip=" + str(skip)

        response = requests.get(request_url)

        return self.response_treat.treatment(response, pretty_response)

    def delete_dataset(self, dataset_name, pretty_response=False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting the dataset. The
        delete operation is always synchronous because it is very fast, since
        the deletion is performed in background. If a dataset was used by
        another task (Ex. projection, histogram, pca, tuning and so forth), it
        cannot be deleted.

        pretty_response: If true return indented string, else return dict.
        dataset_name: Represents the dataset name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        request_url = self.cluster_url + "/" + dataset_name

        response = requests.delete(request_url)

        return self.response_treat.treatment(response, pretty_response)
