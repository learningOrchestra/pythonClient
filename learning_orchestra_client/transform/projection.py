"""

"""
from response_treat import ResponseTreat
import requests
from dataset.dataset import Dataset


class Projection:
    def __init__(self, ip_from_cluster):
        self.cluster_url = "http://" + ip_from_cluster + "/api/learningOrchestra/v1/transform/projection"

    # def delete_dataset_attributes_sync(self):

    # def insert_dataset_attributes_sync(self):

    # def insert_dataset_attribute_sync(self):

    # def reduce_dataset_sync(self):

    # def enlarge_dataset_sync(self):

    # def join_datasets_sync(self):

    # def join_dataset_sync(self):

    # def update_dataset_values_sync(self):

    def search_projections(self, projection_name, pretty_response=False):
        """
        description:  This method is responsible for retrieving a specific projection

        dataset_name: Is the name of the dataset file.
        limit: Number of rows to return in pagination(default: 10) (maximum is set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: Specific projection metadata stored in Learning Orchestra or an error if there is no such projections.
        """
        response = self.search_projections_content(projection_name, limit=1, pretty_response=pretty_response)
        return response

    def search_all_projections(self, pretty_response=False):
        """
        description: This method retrieves all projection metadata, it does not retrieve the projection content.

        return: All projections metadata stored in Learning Orchestra or an empty result.
        """
        cluster_url_projection = self.cluster_url
        response = requests.get(cluster_url_projection)
        return ResponseTreat().treatment(response, pretty_response)

    def search_projections_content(self, projection_name, query="{}", limit=0, skip=0, pretty_response=True):
        """
        description: This method is responsible for retrieving the dataset content

        dataset_name: Is the name of the dataset file.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return A page with some tuples or registers inside or an error if there is no such dataset. The current page
        is also returned to be used in future content requests.
        """
        cluster_url_projection = self.cluster_url + "/" + projection_name + "?query=" + query + "&limit=" + str(
            limit) + "&skip=" + str(skip)
        response = requests.get(cluster_url_projection)
        return ResponseTreat().treatment(response, pretty_response)

    def delete_projections(self, projection_name, pretty_response=False):
        """
        description: This method is responsible for deleting the projection. The delete operation is always synchronous
        because it is very fast, since the deletion is performed in background. If a projection was used by another task
        (Ex. histogram, pca, tuning and so forth), it cannot be deleted.

        projection_name: Represents the projection name.

        return: JSON object with an error message, a warning message or a correct delete message
        """
        cluster_url_projection = self.cluster_url + "/" + projection_name
        response = requests.delete(cluster_url_projection)
        return ResponseTreat().treatment(response, pretty_response)

    def its_ready(self):
        """
        description: This method check if all datasets has finished being inserted into the Learning Orchestra storage
        mechanism.
        """
        operable = self.search_all_projections()
        if '"finished": false' in operable:
            return True

    def insert_dataset_attributes_async(self, dataset_name, projection_name, fields, pretty_response=False):
        if pretty_response:
            print(
                "\n----------"
                + " CREATE PROJECTION FROM "
                + dataset_name
                + " TO "
                + projection_name
                + " ----------"
            )
        request_body = {
            "inputDatasetName": dataset_name,
            "outputDatasetName": projection_name,
            "names": fields,
        }
        Dataset.its_ready(dataset_name, pretty_response)
        request_url = self.cluster_url
        response = requests.post(url=request_url, json=request_body)
        return ResponseTreat().treatment(response, pretty_response)




