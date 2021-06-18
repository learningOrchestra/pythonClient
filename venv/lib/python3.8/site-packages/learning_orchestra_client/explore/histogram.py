from learning_orchestra_client.observe.observe import Observer
from learning_orchestra_client._util._response_treat import ResponseTreat
import requests
from typing import Union
from learning_orchestra_client._util._entity_reader import EntityReader


class ExploreHistogram:
    __INPUT_NAME = "inputDatasetName"
    __OUTPUT_NAME = "outputDatasetName"
    __FIELDS = "names"

    def __init__(self, cluster_ip: str):
        self.__cluster_ip = cluster_ip
        self.__api_path = "/api/learningOrchestra/v1/explore/histogram"
        self.__service_url = f'{cluster_ip}{self.__api_path}'
        self.__response_treat = ResponseTreat()
        self.__observer = Observer(self.__cluster_ip)
        self.__entity_reader = EntityReader(self.__service_url)

    def run_histogram_sync(self,
                           dataset_name: str,
                           histogram_name: str,
                           fields: list,
                           pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method creates a histogram
        synchronously, so the caller waits until the histogram is inserted into
        the Learning Orchestra storage mechanism.

        dataset_name: Represents the name of dataset.
        histogram_name: Represents the name of histogram.
        fields: Represents a list of attributes.
        pretty_response: If true it returns a string, otherwise a dictionary.

        return: A JSON object with error or warning messages. In case of
        success, it returns a histogram.
        """

        request_body = {
            self.__INPUT_NAME: dataset_name,
            self.__OUTPUT_NAME: histogram_name,
            self.__FIELDS: fields,
        }
        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        self.__observer.wait(dataset_name)

        return self.__response_treat.treatment(response, pretty_response)

    def run_histogram_async(self,
                            dataset_name: str,
                            histogram_name: str,
                            fields: list,
                            pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method creates a histogram
        asynchronously, so the caller does not wait until the histogram is
        inserted into the Learning Orchestra storage mechanism.

        dataset_name: Represents the name of dataset.
        histogram_name: Represents the name of histogram.
        fields: Represents a list of attributes.
        pretty_response: If true it returns a string, otherwise a dictionary.

        return: A JSON object with error or warning messages. In case of
        success, it returns a histogram.
        """

        request_body = {
            self.__INPUT_NAME: dataset_name,
            self.__OUTPUT_NAME: histogram_name,
            self.__FIELDS: fields,
        }
        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)

        return self.__response_treat.treatment(response, pretty_response)

    def search_all_histograms(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all histogram metadata, it does not
        retrieve the histogram content.

        pretty_response: If true it returns a string, otherwise a dictionary.

        return: A list with all histogram metadata stored in Learning Orchestra
        or an empty result.
        """

        response = self.__entity_reader.read_all_instances_from_entity()
        return self.__response_treat.treatment(response, pretty_response)

    def search_histogram_content(self,
                                 histogram_name: str,
                                 query: dict = {},
                                 limit: int = 10,
                                 skip: int = 0,
                                 pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for retrieving the histogram
        content.

        pretty_response: If true it returns a string, otherwise a dictionary.
        histogram_name: Represents the histogram name.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: A page with some tuples or registers inside or an error if there
        is no such projection. The current page is also returned to be used in
        future content requests.
        """

        response = self.__entity_reader.read_entity_content(
            histogram_name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def delete_histogram(self, histogram_name: str,
                         pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method is responsible for deleting a histogram.
        The delete operation is always asynchronous,
        since the deletion is performed in background.

        pretty_response: If true it returns a string, otherwise a dictionary.
        histogram_name: Represents the histogram name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        cluster_url_histogram = f'{self.__service_url}/{histogram_name}'
        response = requests.delete(cluster_url_histogram)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the run_histogram_async method or delete_histogram
           method.

           name: Represents the histogram name.
           timeout: Represents the time in seconds to wait for a histogram to
           finish its run.

           return: JSON object with an error message, a warning message or a
           correct histogram result
        """
        return self.__observer.wait(name, timeout)
