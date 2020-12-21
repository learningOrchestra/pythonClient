from ..response_treat import ResponseTreat
from ..dataset import Dataset
from ..observer import Observer
import requests
from typing import Union


class Projection:
    def __init__(self, ip_from_cluster: str):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/transform/projection"
        self.METADATA_INDEX = 0
        self.response_treat = ResponseTreat()
        self.INPUT_NAME = "inputDatasetName"
        self.OUTPUT_NAME = "outputDatasetName"
        self.FIELDS = "names"
        self.dataset = Dataset(ip_from_cluster)
        self.CLUSTER_IP = ip_from_cluster

    def insert_dataset_attributes_sync(self,
                                       dataset_name: str,
                                       projection_name: str,
                                       fields: list,
                                       pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method inserts a set of attributes into a dataset
        synchronously, the caller waits until the projection is inserted into
        the Learning Orchestra storage mechanism.

        pretty_response: If true return indented string, else return dict.
        projection_name: Represents the projection name.
        dataset_name: Represents the dataset name.
        fields: Represents the set of attributes to be inserted. This is list
        with all attributes.

        return: A JSON object with error or warning messages. In case of
        success, it returns the projection metadata.
        """

        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: projection_name,
            self.FIELDS: fields,
        }
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        Observer(projection_name, self.CLUSTER_IP).observe_processing(
            pretty_response)

        if pretty_response:
            print(
                "\n----------"
                + " CREATE PROJECTION FROM "
                + dataset_name
                + " TO "
                + projection_name
                + " ----------"
            )

        return self.response_treat.treatment(response, pretty_response)

    def insert_dataset_attributes_async(self,
                                        dataset_name: str,
                                        projection_name: str,
                                        fields: list,
                                        pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method inserts a set of attributes into a dataset
        asynchronously, the caller does not wait until the projections is
        inserted into the Learning Orchestra storage mechanism. Instead,
        the caller receives a JSON object with a URL to proceed future calls
        to verify if the projection was inserted.

        pretty_response: If true return indented string, else return dict.
        projection_name: Represents the projection name.
        dataset_name: Represents the dataset name.
        fields: Represents the set of attributes to be inserted. This is list
        with all attributes.

        return: A JSON object with error or warning messages. In case of
        success, it returns the projection metadata.
        """

        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: projection_name,
            self.FIELDS: fields,
        }
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        if pretty_response:
            print(
                "\n----------"
                + " CREATE PROJECTION FROM "
                + dataset_name
                + " TO "
                + projection_name
                + " ----------"
            )

        return self.response_treat.treatment(response, pretty_response)

    def delete_dataset_attributes_sync(self,
                                       dataset_name: str,
                                       projection_name: str,
                                       fields_to_delete: list,
                                       pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method delete a set of attributes on a dataset
        synchronously, the caller waits until the projection is inserted into
        the Learning Orchestra storage mechanism.

        pretty_response: If true return indented string, else return dict.
        projection_name: Represents the projection name.
        dataset_name: Represents the dataset name.
        fields_to_delete: Represents the set of attributes to be inserted.
        This is list with all attributes.

        return: A JSON object with error or warning messages. In case of
        success, it returns the projection metadata.
        """
        dataset_metadata = self.search_projections_content(dataset_name,
                                                           limit=1)

        fields_to_insert = dataset_metadata.get('result')[self.METADATA_INDEX] \
            .get('fields')
        for field in fields_to_delete:
            fields_to_insert.remove(field)

        response = self.insert_dataset_attributes_sync(dataset_name,
                                                       projection_name,
                                                       fields_to_insert,
                                                       pretty_response)

        return response

    def delete_dataset_attributes_async(self,
                                        dataset_name: str,
                                        projection_name: str,
                                        fields_to_delete: list,
                                        pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method delete a set of attributes into a dataset
        asynchronously, the caller does not wait until the projections is
        inserted into the Learning Orchestra storage mechanism. Instead,
        the caller receives a JSON object with a URL to proceed future calls
        to verify if the projection was inserted.

        pretty_response: If true return indented string, else return dict.
        projection_name: Represents the projection name.
        dataset_name: Represents the dataset name.
        fields_to_delete: Represents the set of attributes to be inserted.
        This is list with all attributes.

        return: A JSON object with error or warning messages. In case of
        success, it returns the projection metadata.
        """
        dataset_metadata = self.search_projections_content(dataset_name,
                                                           limit=1)

        fields_to_insert = dataset_metadata.get('result')[self.METADATA_INDEX] \
            .get('fields')
        for field in fields_to_delete:
            fields_to_insert.remove(field)

        response = self.insert_dataset_attributes_async(dataset_name,
                                                        projection_name,
                                                        fields_to_insert,
                                                        pretty_response)

        return response

    def search_all_projections(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all projection metadata, it does not
        retrieve the projection content.

        pretty_response: If true return indented string, else return dict.

        return: A list with all projections metadata stored in Learning
        Orchestra or an empty result.
        """
        cluster_url_projection = self.cluster_url

        response = requests.get(cluster_url_projection)

        return self.response_treat.treatment(response, pretty_response)

    def search_projections(self, projection_name: str,
                           pretty_response: bool = False) -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving a specific
        projection.

        pretty_response: If true return indented string, else return dict.
        projection_name: Represents the projection name.
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: Specific projection metadata stored in Learning Orchestra or an
        error if there is no such projections.
        """
        pretty = pretty_response

        response = self.search_projections_content(projection_name, limit=1,
                                                   pretty_response=pretty)

        return response

    def search_projections_content(self,
                                   projection_name: str,
                                   query: dict = {},
                                   limit: int = 10,
                                   skip: int = 0,
                                   pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for retrieving the dataset
        content.

        pretty_response: If true return indented string, else return dict.
        projection_name: Represents the projection name.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: A page with some tuples or registers inside or an error if there
        is no such projection. The current page is also returned to be used in
        future content requests.
        """

        cluster_url_projection = self.cluster_url + "/" + projection_name + \
                                 "?query=" + str(query) + \
                                 "&limit=" + str(limit) + \
                                 "&skip=" + str(skip)

        response = requests.get(cluster_url_projection)

        return self.response_treat.treatment(response, pretty_response)

    def delete_projections(self, projection_name: str,
                           pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting a projection.
        The delete operation is always synchronous because it is very fast,
        since the deletion is performed in background. If a projection was
        used by another task (Ex. histogram, pca, tuning and so forth),
        it cannot be deleted.

        pretty_response: If true return indented string, else return dict.
        projection_name: Represents the projection name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """
        cluster_url_projection = self.cluster_url + "/" + projection_name

        response = requests.delete(cluster_url_projection)

        return self.response_treat.treatment(response, pretty_response)
