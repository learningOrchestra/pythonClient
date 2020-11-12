"""

"""
__author__ = "Otavio Henrique Rodrigues Mapa & Matheus Goncalves Ribeiro"
__credits__ = "all free source developers"
__status__ = "Prototype"

import requests
import json
import time
import ast

from response_treat import ResponseTreat


class Dataset:

    def __init__(self, ip_from_cluster):
        # global cluster_url
        self.cluster_url = "http://" + ip_from_cluster + "/api/learningOrchestra/v1/dataset"

    def insert_dataset_sync(self, dataset_name, url, pretty_response=True):
        waiting = False
        waiting = self.its_not_ready()
        if waiting:
            print("wait for process other dataset")
        else:
            cluster_url_dataset = self.cluster_url
            request_body = {"datasetName": dataset_name, "url": url}
            response = requests.post(url=cluster_url_dataset, json=request_body)
            print("\n----------" + " CREATE FILE " + dataset_name + " ----------")
            return ResponseTreat().treatment(response, pretty_response)

    def insert_dataset_async(self, dataset_name, url, pretty_response=True):
        """
        description: This method is responsible to insert a dataset from a URI asynchronously, i.e., the caller does
        not wait until the dataset is inserted into the Learning Orchestra storage mechanism. Instead, the caller
        receives a JSON object with a URL to proceed future calls to verify if the dataset is inserted.

        dataset_name: Is the name of the dataset file that will be created.
        url: Url to CSV file.

        return: A JSON object with an error or warning message or a URL indicating the correct operation (the caller
        must use such an URL to proceed future checks to verify if the dataset is inserted).
        """
        cluster_url_dataset = self.cluster_url
        request_body = {"datasetName": dataset_name, "url": url}
        response = requests.post(url=cluster_url_dataset, json=request_body)
        print("\n----------" + " CREATE FILE " + dataset_name + " ----------")
        return ResponseTreat().treatment(response, pretty_response)

    def search_all_datasets(self, pretty_response=True):
        """
        description: This method retrieves all datasets metadata, i.e., it does not retrieve the dataset content.
        dataset_name: Is the name of the dataset file.

        return: All datasets metadata stored in Learning Orchestra or an empty result.
        """
        cluster_url_dataset = self.cluster_url
        response = requests.get(cluster_url_dataset)
        return ResponseTreat().treatment(response, pretty_response)

    def search_dataset(self, dataset_name, pretty_response=True):
        response = self.search_dataset_content(dataset_name, limit=1, pretty_response=pretty_response)
        return response

    # def update_dataset_sync(self, dataset_name, data_type:

    # def update_dataset_async(self, dataset_name, data_type:

    def search_dataset_content(self, dataset_name, query="{}", limit=0, skip=0, pretty_response=True):
        """
        description:  This method is responsible for retrieving the dataset content

        dataset_name: Is the name of the dataset file.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return A page with some tuples or registers inside or an error if there is no such dataset. The current page
        is also returned to be used in future content requests.
        """
        cluster_url_dataset = self.cluster_url + "/" + dataset_name + "?query=" + query + "&limit=" + str(
            limit) + "&skip=" + str(skip)
        response = requests.get(cluster_url_dataset)
        return ResponseTreat().treatment(response, pretty_response)

    def delete_dataset(self, dataset_name, pretty_response=True):
        """
        description: This method is responsible for deleting the dataset. The delete operation is always synchronous
        because it is very fast, since the deletion is performed in background. If a dataset was used by another task
        (Ex. projection, histogram, pca, tuning and so forth), it cannot be deleted.

        dataset_name: Represents the dataset name.

        return: JSON object with an error message, a warning message or a correct delete message
        """
        cluster_url_dataset = self.cluster_url + "/" + dataset_name
        response = requests.delete(cluster_url_dataset)
        return ResponseTreat().treatment(response, pretty_response)

    def its_not_ready(self):
        operable = self.search_all_datasets()
        if '"finished": false' in operable:
            return True
