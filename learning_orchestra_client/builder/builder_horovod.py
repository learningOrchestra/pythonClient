from learning_orchestra_client.observe.observe import Observer
from learning_orchestra_client._util._response_treat import ResponseTreat
from learning_orchestra_client._util._entity_reader import EntityReader
import requests
from typing import Union


class BuilderHorovod:
    __BUILDER_NAME = 'name',
    __DESCRIPTION_NAME = 'description'
    __MODELING_CODE_NAME = 'code',
    __PARAMETERS = 'parameters'

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/builder/horovod"
        self.__service_url = f'{cluster_ip}{self.__api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def run_horovod_sync(self, builder_name: str, modeling_code: str, parameters: dict = dict({}),
                         description: str = '',
                         pretty_response: bool = False) -> \
            Union[dict, str]:
        """
                description: This method call runs several steps of a machine
                learning pipeline (transform, tune, train and evaluate, for instance)
                using a model code. It represents a way to run
                an entire pipeline. The caller waits until the method execution ends,
                since it is a synchronous method.

                modeling_code: Represent python3 code used to build the pipeline.
                builder_name: Represent the builder name that is accessible later via API.
                parameters: Represent parameters in the cluster that is needed on the pipeline

                pretty_response: if True it represents a result useful for visualization

                return: The response of such python3 code.
                """

        request_body_content = {
            self.__DESCRIPTION_NAME: description,
            self.__BUILDER_NAME: builder_name,
            self.__MODELING_CODE_NAME: modeling_code,
            self.__PARAMETERS: parameters,
        }

        response = requests.post(url=self.__service_url,
                                 json=request_body_content)

        self.__observer.wait(builder_name)

        return self.__response_treat.treatment(response, pretty_response)

    def run_horovod_async(self, builder_name: str, modeling_code: str, parameters: dict = dict({}),
                          description: str = '',
                          pretty_response: bool = False) -> Union[dict, str]:
        """
                description: This method call runs several steps of a machine
                learning pipeline (transform, tune, train and evaluate, for instance)
                using a model code. It represents a way to run
                an entire pipeline. The caller waits until the method execution ends,
                since it is a synchronous method.

                modeling_code: Represent python3 code used to build the pipeline.
                builder_name: Represent the builder name that is accessible later via API.
                parameters: Represent parameters in the cluster that is needed on the pipeline

                pretty_response: if True it represents a result useful for visualization

                return: The response of such python3 code.
                """

        request_body_content = {
            self.__DESCRIPTION_NAME: description,
            self.__BUILDER_NAME: builder_name,
            self.__MODELING_CODE_NAME: modeling_code,
            self.__PARAMETERS: parameters,
        }

        response = requests.post(url=self.__service_url,
                                 json=request_body_content)

        return self.__response_treat.treatment(response, pretty_response)

    def get_builder(self, builder_name: str, query: dict = {}, limit: int = 10, skip: int = 0,
                    pretty_response: bool = False) \
            -> Union[dict, str]:
        """
                description: This method is responsible for retrieving the model
                predictions content.

                pretty_response: If true it returns a string, otherwise a dictionary.
                builder_name: Represents the model predictions name.
                query: Query to make in MongoDB(default: empty query)
                limit: Number of rows to return in pagination(default: 10) (maximum is
                set at 20 rows per request)
                skip: Number of rows to skip in pagination(default: 0)

                return: A page with some tuples or registers inside or an error if the
                pipeline runs incorrectly. The current page is also returned to be used
                in future content requests.
                """
        response = self.__entity_reader.read_entity_content(
            builder_name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def delete_builder(self, builder_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting a model prediction.
        The delete operation is always asynchronous,
        since the deletion is performed in background.

        pretty_response: If true it returns a string, otherwise a dictionary.
        builder_name: Represents the pipeline name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        cluster_url_dataset = f'{self.__service_url}/{builder_name}'

        response = requests.delete(cluster_url_dataset)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, builder_name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the run_horovod_async method.

           dataset_name: Represents the pipeline name.
           timeout: Represents the time in seconds to wait for a builder to
           finish its run.

           return: JSON object with an error message, a warning message or a
           correct execution of a pipeline
        """
        return self.__observer.wait(builder_name, timeout)
