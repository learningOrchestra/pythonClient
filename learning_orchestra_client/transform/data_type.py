from observer import Observer
from response_treat import ResponseTreat
from dataset.dataset import Dataset
import requests


class DataType:
    def __init__(self, ip_from_cluster):
        self.CLUSTER_IP = ip_from_cluster
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/transform/dataType"
        self.ResponseTreat = ResponseTreat()
        self.Dataset = Dataset(ip_from_cluster)
        self.INPUT_NAME = "inputDatasetName"
        self.TYPES = "types"

    def update_dataset_types(self, dataset_name, fields_change,
                             pretty_response=False):
        """
        description: Change types of fields to number or string.

        dataset_name: Represents the dataset name.
        fields_change: Fields to change with types.

        return: A JSON object with error or warning messages.
        """
        Observer(dataset_name, self.CLUSTER_IP).observe_processing(
                 pretty_response)
        url_request = self.cluster_url
        body_request = {
            self.INPUT_NAME: dataset_name,
            self.TYPES: fields_change
        }
        response = requests.patch(url=url_request, json=body_request)
        return self.ResponseTreat.treatment(response, pretty_response)
