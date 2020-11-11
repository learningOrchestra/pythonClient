"""

"""
__author__ = "Otávio Henrique Rodrigues Mapa & Matheus Gonçalves Ribeiro"
__credits__ = "all free source developers"
__license__ = "GNU General Public License"
__version__ = "1.0.0"
__status__ = "Prototype"

import requests
import json
import time

cluster_url = None


class Dataset:

    def __init__(self, ip_from_cluster):
        global cluster_url
        self.cluster_url = "http://" + ip_from_cluster

    def post(self, dataset_name, url):
        """ description: download a file on your cluster
        dataset_name: name of file
        url: url to CSV file
        """
        cluster_url_dataset = self.cluster_url + "/api/learningOrchestra/v1/dataset"
        request_body = {"datasetName": dataset_name, "url": url}
        response = requests.post(url=cluster_url_dataset, json=request_body)
        print("\n----------" + " CREATE FILE " + dataset_name + " ----------")
        print(response.json())

    def get_dataset_list(self):
        """ description: show a list with dataset file names
        """
        cluster_url_dataset = self.cluster_url + "/api/learningOrchestra/v1/dataset"
        response = requests.get(cluster_url_dataset)
        print(response.json())

    def get_dataset(self, dataset_name):
        """ description: read a dataset
        dataset_name: name of file
        """
        cluster_url_dataset = self.cluster_url + "/api/learningOrchestra/v1/dataset/" + dataset_name + "?query={}&limit=5&skip=0"
        response = requests.get(cluster_url_dataset)
        print(response.json())

    def delete(self, dataset_name):
        """ description: delete a dataset
            dataset_name: name of file
        """
        cluster_url_dataset = self.cluster_url + "/api/learningOrchestra/v1/dataset/" + dataset_name
        response = requests.delete(cluster_url_dataset)
        print(response.json())
