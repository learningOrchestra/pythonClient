from learning_orchestra_client._util._response_treat import ResponseTreat
from learning_orchestra_client.observe.observe import Observer
from learning_orchestra_client._util._entity_reader import EntityReader
import requests
from typing import Union


class TransformProjection:
    __INPUT_NAME = "inputDatasetName"
    __OUTPUT_NAME = "outputDatasetName"
    __FIELDS = "names"

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/transform/projection"
        self.__service_url = f'{cluster_ip}{self.__api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def remove_dataset_attributes_sync(self,
                                       dataset_name: str,
                                       projection_name: str,
                                       fields: list,
                                       pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method removes a set of attributes of a dataset
        synchronously, the caller waits until the projection is inserted into
        the Learning Orchestra storage mechanism.

        pretty_response: If returns true a string, otherwise a dictionary.
        projection_name: Represents the projection name.
        dataset_name: Represents the dataset name.
        fields: Represents the set of attributes to be removed. This is list
        with some attributes.

        return: A JSON object with error or warning messages. In case of
        success, it returns the projection metadata.
        """

        request_body = {
            self.__INPUT_NAME: dataset_name,
            self.__OUTPUT_NAME: projection_name,
            self.__FIELDS: fields,
        }
        request_url = self.__service_url
        response = requests.post(url=request_url, json=request_body)
        self.__observer.wait(dataset_name)

        return self.__response_treat.treatment(response, pretty_response)

    def remove_dataset_attributes_async(self,
                                        dataset_name: str,
                                        projection_name: str,
                                        fields: list,
                                        pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method removes a set of attributes of a dataset
        asynchronously; this way, the caller does not wait until the projection
        is inserted into the Learning Orchestra storage mechanism. A wait
        method call must occur to guarantee a synchronization barrier.

        pretty_response: If returns true a string, otherwise a dictionary.
        projection_name: Represents the projection name.
        dataset_name: Represents the dataset name.
        fields: Represents the set of attributes to be removed. This is list
        with some attributes.

        return: A JSON object with error or warning messages. In case of
        success, it returns the projection URL to be obtained latter with a
        wait method call.
        """

        request_body = {
            self.__INPUT_NAME: dataset_name,
            self.__OUTPUT_NAME: projection_name,
            self.__FIELDS: fields,
        }
        request_url = self.__service_url
        response = requests.post(url=request_url, json=request_body)

        return self.__response_treat.treatment(response, pretty_response)

    def search_all_projections(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all projection metadata, i.e., it
        does not retrieve the projection content.

        pretty_response: If true it returns a string, otherwise a dictionary.

        return: A list with all projections metadata stored in Learning
        Orchestra or an empty result.
        """
        response = self.__entity_reader.read_all_instances_from_entity()
        return self.__response_treat.treatment(response, pretty_response)

    def search_projection_content(self,
                                  projection_name: str,
                                  query: dict = {},
                                  limit: int = 10,
                                  skip: int = 0,
                                  pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for retrieving the projection
        content.

        pretty_response: If true it returns a string, otherwise a dictionary.
        projection_name: Represents the projection name.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: A page with some tuples or registers inside or an error if there
        is no such projection. The current page is also returned to be used in
        future content requests.
        """

        response = self.__entity_reader.read_entity_content(
            projection_name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def delete_projection(self, projection_name: str,
                          pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting a projection.
        The delete operation is always asynchronous and performed in background.


        pretty_response: If true it returns a string, otherwise a dictionary.
        projection_name: Represents the projection name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """
        cluster_url_projection = f'{self.__service_url}/{projection_name}'

        response = requests.delete(cluster_url_projection)
        response.raise_for_status()

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, projection_name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the remove_dataset_attributes_async method,
           delete_projection method.

           name: Represents the projection name.
           timeout: Represents the time in seconds to wait for a projection to
           finish its run.

           return: JSON object with an error message, a warning message or a
           correct projection result
        """
        return self.__observer.wait(projection_name, timeout)
