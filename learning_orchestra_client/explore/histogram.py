from ..observer import Observer
from ..response_treat import ResponseTreat
from ..dataset import Dataset
import requests
from typing import Union


class Histogram:
    def __init__(self, ip_from_cluster: str):
        self.CLUSTER_IP = ip_from_cluster
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/explore/histogram"
        self.response_treat = ResponseTreat()
        self.INPUT_NAME = "inputDatasetName"
        self.OUTPUT_NAME = "outputDatasetName"
        self.FIELDS = "names"
        self.dataset = Dataset(ip_from_cluster)

    def run_histogram_sync(self,
                           dataset_name: str,
                           histogram_name: str,
                           fields: list,
                           pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method run histogram algorithm to create a histogram
        synchronously, the caller waits until the histogram is inserted into
        the Learning Orchestra storage mechanism.

        dataset_name: Represents the name of dataset.
        histogram_name: Represents the name of histogram.
        fields: Represents a list of attributes.
        pretty_response: If true return indented string, else return dict.

        return: A JSON object with error or warning messages. In case of
        success, it returns a histogram.
        """

        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: histogram_name,
            self.FIELDS: fields,
        }
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        Observer(histogram_name, self.CLUSTER_IP).observe_processing(
            pretty_response)

        if pretty_response:
            print(
                "\n----------"
                + " CREATE HISTOGRAM FROM "
                + dataset_name
                + " TO "
                + histogram_name
                + " ----------"
            )

        return self.response_treat.treatment(response, pretty_response)

    def run_histogram_async(self,
                            dataset_name: str,
                            histogram_name: str,
                            fields: list,
                            pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method run histogram algorithm to create a histogram
        asynchronously, the caller does not wait until the histogram is
        inserted into the Learning Orchestra storage mechanism. Instead,
        the caller receives a JSON object with a URL to proceed future calls
        to verify if the histogram was inserted.

        dataset_name: Represents the name of dataset.
        histogram_name: Represents the name of histogram.
        fields: Represents a list of attributes.
        pretty_response: If true return indented string, else return dict.

        return: A JSON object with error or warning messages. In case of
        success, it returns a histogram.
        """

        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: histogram_name,
            self.FIELDS: fields,
        }
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        if pretty_response:
            print(
                "\n----------"
                + " CREATE HISTOGRAM FROM "
                + dataset_name
                + " TO "
                + histogram_name
                + " ----------"
            )

        return self.response_treat.treatment(response, pretty_response)

    def search_all_histograms(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all histogram names, it does not
        retrieve the histogram content.

        pretty_response: If true return indented string, else return dict.

        return: A list with all histogram names stored in Learning Orchestra
        or an empty result.
        """

        cluster_url_histogram = self.cluster_url

        response = requests.get(cluster_url_histogram)

        return self.response_treat.treatment(response, pretty_response)

    def search_histogram_data(self,
                              histogram_name: str,
                              query: dict = {},
                              limit: int = 10,
                              skip: int = 0,
                              pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for retrieving the histogram
        content.

        pretty_response: If true return indented string, else return dict.
        histogram_name: Represents the histogram name.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: A page with some tuples or registers inside or an error if there
        is no such projection. The current page is also returned to be used in
        future content requests.
        """

        cluster_url_histogram = self.cluster_url + "/" + histogram_name + \
                                "?query=" + str(query) + \
                                "&limit=" + str(limit) + \
                                "&skip=" + str(skip)

        response = requests.get(cluster_url_histogram)

        return self.response_treat.treatment(response, pretty_response)

    def delete_histogram(self, histogram_name: str,
                         pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method is responsible for deleting a histogram.
        The delete operation is always synchronous because it is very fast,
        since the deletion is performed in background.

        pretty_response: If true return indented string, else return dict.
        histogram_name: Represents the histogram name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        cluster_url_histogram = self.cluster_url + "/" + histogram_name

        response = requests.delete(cluster_url_histogram)

        return self.response_treat.treatment(response, pretty_response)
