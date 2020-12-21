from ..response_treat import ResponseTreat
from ..dataset import Dataset
import requests
from typing import Union


class DataType:
    def __init__(self, ip_from_cluster):
        self.CLUSTER_IP = ip_from_cluster
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/transform/dataType"
        self.ResponseTreat = ResponseTreat()
        self.Dataset = Dataset(ip_from_cluster)
        self.INPUT_NAME = "inputDatasetName"
        self.TYPES = "types"

    def update_dataset_types(self,
                             dataset_name: str,
                             fields_change: dict,
                             pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: Change types of fields to number or string.

        dataset_name: Represents the dataset name.
        fields_change: Fields to change with types. This is a dict with each
        key:value being field:type

        return: A JSON object with error or warning messages.
        """

        url_request = self.cluster_url
        body_request = {
            self.INPUT_NAME: dataset_name,
            self.TYPES: fields_change
        }

        response = requests.patch(url=url_request, json=body_request)

        return self.ResponseTreat.treatment(response, pretty_response)
