import time

import requests

from response_treat import ResponseTreat
from dataset.dataset import Dataset


class Histogram:
    def __init__(self, ip_from_cluster):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/explore/histogram"
        self.WAIT_TIME = 3
        self.METADATA_INDEX = 0
        self.response_treat = ResponseTreat()
        self.INPUT_NAME = "inputDatasetName"
        self.OUTPUT_NAME = "outputDatasetName"
        self.FIELDS = "names"
        self.dataset = Dataset(ip_from_cluster)

    def run_histogram_sync(self, dataset_name, histogram_name, fields,
                           pretty_response=False):
        """
        description: This method run histogram algorithm to create a histogram
        synchronously, the caller waits until the histogram is inserted into
        the Learning Orchestra storage mechanism.

        dataset_name: Represents the name of dataset.
        histogram_name: Represents the name of histogram.
        fields: Represents the set of attributes.
        pretty_response: If true return indented string, else return dict.

        return: A JSON object with error or warning messages. In case of
        success, it returns a histogram.
        """
        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: histogram_name,
            self.FIELDS: fields,
        }
        self.dataset.verify_dataset_processing_done(dataset_name,
                                                    pretty_response)
        request_url = self.cluster_url
        response = requests.post(url=request_url, json=request_body)
        self.verify_dataset_histogram_done(histogram_name, pretty_response)
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

    def run_histogram_async(self, dataset_name, histogram_name, fields,
                            pretty_response=False):
        """
        description: This method run histogram algorithm to create a histogram
        asynchronously, the caller does not wait until the histogram is
        inserted into the Learning Orchestra storage mechanism. Instead,
        the caller receives a JSON object with a URL to proceed future calls
        to verify if the histogram was inserted.

        dataset_name: Represents the name of dataset.
        histogram_name: Represents the name of histogram.
        fields: Represents the set of attributes.
        pretty_response: If true return indented string, else return dict.

        return: A JSON object with error or warning messages. In case of
        success, it returns a histogram.
        """
        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: histogram_name,
            self.FIELDS: fields,
        }
        self.dataset.verify_dataset_processing_done(dataset_name,
                                                    pretty_response)
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

    def search_all_histograms(self, pretty_response=False):
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

    def search_histogram_data(self, histogram_name, query={}, limit=10, skip=0,
                              pretty_response=False):
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

    def verify_dataset_histogram_done(self, histogram_name,
                                      pretty_response=True):
        """
        description: This method check from time to time using Time lib, if a
        histogram has finished being inserted into the Learning Orchestra
        storage mechanism.

        histogram_name: Represents the histogram name.
        pretty_response: If true return indented string, else return dict.
        """
        if pretty_response:
            print(
                "\n---------- WAITING " + histogram_name + " FINISH ----------")
        while True:
            time.sleep(self.WAIT_TIME)
            response = self.search_histogram_data(histogram_name, limit=1,
                                                  pretty_response=False)
            if len(response["result"]) == 0:
                continue
            if response["result"][self.METADATA_INDEX]["finished"]:
                break

    def delete_histogram(self, histogram_name, pretty_response=False):
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