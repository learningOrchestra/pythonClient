from response_treat import ResponseTreat
from dataset.dataset import Dataset
import requests
import time


class Projection:
    def __init__(self, ip_from_cluster):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/transform/projection"
        self.WAIT_TIME = 3
        self.METADATA_INDEX = 0
        self.response_treat = ResponseTreat()
        self.INPUT_NAME = "inputDatasetName"
        self.OUTPUT_NAME = "outputDatasetName"
        self.FIELDS = "names"
        self.dataset = Dataset(ip_from_cluster)

    def delete_dataset_attributes_sync(self, dataset_name, projection_name,
                                       fields_to_delete, pretty_response=False):
        """
        description: This method delete a set of attributes into a dataset.

        pretty_response: If true return indented string, else return dict.
        datasetName: Represents the dataset name.
        fields: Represents the set of attributes to be inserted.

        return: A JSON object with error or warning messages. In case of
        success, it returns the dataset metadata.
        """
        dataset_metadata = self.search_projections_content(dataset_name,
                                                           limit=1)
        fields_to_insert = dataset_metadata.get('result')[self.METADATA_INDEX]\
                                           .get('fields')
        for field in fields_to_delete:
            fields_to_insert.remove(field)
        response = self.insert_dataset_attributes_sync(dataset_name,
                                                       projection_name,
                                                       fields_to_insert,
                                                       pretty_response)
        return response

    def insert_dataset_attributes_sync(self, dataset_name, projection_name,
                                       fields, pretty_response=False):
        """
        description: This method inserts a set of attributes into a dataset.

        pretty_response: If true return indented string, else return dict.
        datasetName: Represents the dataset name.
        fields: Represents the set of attributes to be inserted.

        return: A JSON object with error or warning messages. In case of
        success, it returns the dataset metadata.
        """
        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: projection_name,
            self.FIELDS: fields,
        }
        self.dataset.verify_dataset_processing_done(dataset_name,
                                                    pretty_response)
        request_url = self.cluster_url
        response = requests.post(url=request_url, json=request_body)
        self.verify_projection_processing_done(projection_name, pretty_response)
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

    # def insert_dataset_attribute_sync(self):

    # def reduce_dataset_sync(self):

    # def enlarge_dataset_sync(self):

    # def join_datasets_sync(self):

    # def join_dataset_sync(self):

    # def update_dataset_values_sync(self):

    def search_projections(self, projection_name, pretty_response=False):
        """
        description:  This method is responsible for retrieving a specific
        projection

        pretty_response: If true return indented string, else return dict.
        dataset_name: Is the name of the dataset file.
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

    def search_all_projections(self, pretty_response=False):
        """
        description: This method retrieves all projection metadata, it does not
        retrieve the projection content.

        pretty_response: If true return indented string, else return dict.

        return: All projections metadata stored in Learning Orchestra or an
        empty result.
        """
        cluster_url_projection = self.cluster_url
        response = requests.get(cluster_url_projection)
        return self.response_treat.treatment(response, pretty_response)

    def search_projections_content(self, projection_name, query={}, limit=10,
                                   skip=0, pretty_response=False):
        """
        description: This method is responsible for retrieving the dataset
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

        cluster_url_projection = self.cluster_url + "/" + projection_name + \
                                                    "?query=" + str(query) + \
                                                    "&limit=" + str(limit) + \
                                                    "&skip=" + str(skip)
        response = requests.get(cluster_url_projection)
        return self.response_treat.treatment(response, pretty_response)

    def delete_projections(self, projection_name, pretty_response=False):
        """
        description: This method is responsible for deleting the projection.
        The delete operation is always synchronous because it is very fast,
        since the deletion is performed in background. If a projection was used
        by another task
        (Ex. histogram, pca, tuning and so forth), it cannot be deleted.

        pretty_response: If true return indented string, else return dict.
        projection_name: Represents the projection name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """
        cluster_url_projection = self.cluster_url + "/" + projection_name
        response = requests.delete(cluster_url_projection)
        return self.response_treat.treatment(response, pretty_response)

    def verify_projection_processing_done(self, projection_name,
                                          pretty_response=False):
        """
        description: This method check from time to time using Time lib, if a
        projection has finished being inserted into the Learning Orchestra
        storage mechanism.

        pretty_response: If true return indented string, else return dict.
        """
        if pretty_response:
            print(
                "\n---------- WAITING " + projection_name + " FINISH -------"
                                                            "---")
        while True:
            time.sleep(self.WAIT_TIME)
            response = self.search_projections_content(projection_name, limit=1,
                                                       pretty_response=False)
            if len(response["result"]) == 0:
                continue
            if response["result"][self.METADATA_INDEX]["finished"]:
                break

    def delete_dataset_attributes_async(self, dataset_name,
                                        projection_name,
                                        fields_to_delete,
                                        pretty_response=False):
        """
        description: This method delete a set of attributes into a dataset.

        pretty_response: If true return indented string, else return dict.
        datasetName: Represents the dataset name.
        fields: Represents the set of attributes to be inserted.

        return: A JSON object with error or warning messages. In case of
        success, it returns the dataset metadata.
        """
        dataset_metadata = self.search_projections_content(dataset_name,
                                                           limit=1)
        fields_to_insert = dataset_metadata.get('result')[self.METADATA_INDEX]\
                                           .get('fields')
        for field in fields_to_delete:
            fields_to_insert.remove(field)
        response = self.insert_dataset_attributes_async(dataset_name,
                                                        projection_name,
                                                        fields_to_insert,
                                                        pretty_response)
        return response

    def insert_dataset_attributes_async(self, dataset_name, projection_name,
                                        fields, pretty_response=False):
        """
        description: This method inserts a set of attributes into a dataset.

        pretty_response: If true return indented string, else return dict.
        datasetName: Represents the dataset name.
        fields: Represents the set of attributes to be inserted.

        return: A JSON object with error or warning messages. In case of
        success, it returns the dataset metadata.
        """
        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: projection_name,
            self.FIELDS: fields,
        }
        self.dataset.verify_dataset_processing_done(dataset_name,
                                                    pretty_response)
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
