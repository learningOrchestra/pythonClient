from .observer import Observer
from ._response_treat import ResponseTreat
from ._entity_reader import EntityReader
import requests
from typing import Union


class BuilderSparkMl:
    __TRAIN_FIELD = "trainDatasetName"
    __TEST_FIELD = "testDatasetName"
    __CODE_FIELD = "modelingCode"
    __CLASSIFIERS_LIST_FIELD = "classifiersList"

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/builder/sparkml"
        self.__service_url = f'{cluster_ip}{self.__api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def run_spark_ml_sync(self,
                          train_dataset_name: str,
                          test_dataset_name: str,
                          modeling_code: str,
                          model_classifiers: list,
                          pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method resource join several steps of machine
        learning workflow (transform, tune, train and evaluate) coupling in
        a unique resource, builder creates several model predictions using
        your own modeling code using a defined set of classifiers. This is made
        synchronously, the caller waits until the model predictions are inserted
        into the Learning Orchestra storage mechanism.

        train_dataset_name: Represent final train dataset.
        test_dataset_name: Represent final test dataset.
        modeling_code: Represent Python3 code for pyspark preprocessing model
        model_classifiers: list of initial classifiers to be used in model
        pretty_response: returns indented string for visualization if True

        return: The resulted predictions URIs.
        """

        request_body_content = {
            self.__TRAIN_FIELD: train_dataset_name,
            self.__TEST_FIELD: test_dataset_name,
            self.__CODE_FIELD: modeling_code,
            self.__CLASSIFIERS_LIST_FIELD: model_classifiers,
        }
        response = requests.post(url=self.__service_url,
                                 json=request_body_content)

        for classifier in model_classifiers:
            self.__observer.wait(f'{test_dataset_name}{classifier}')

        return self.__response_treat.treatment(response, pretty_response)

    def run_spark_ml_async(self,
                           train_dataset_name: str,
                           test_dataset_name: str,
                           modeling_code: str,
                           model_classifiers: list,
                           pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method resource join several steps of machine
        learning workflow (transform, tune, train and evaluate) coupling in
        a unique resource, builder creates several model predictions using
        your own modeling code using a defined set of classifiers. This is made
        asynchronously, the caller does not wait until the model predictions are
        inserted into the Learning Orchestra storage mechanism. Instead, the
        caller receives a JSON object with a URL to proceed future calls to
        verify if the model predictions are inserted.

        train_dataset_name: Represent final train dataset.
        test_dataset_name: Represent final test dataset.
        modeling_code: Represent Python3 code for pyspark preprocessing model
        model_classifiers: list of initial classifiers to be used in model
        pretty_response: returns indented string for visualization if True

        return: The resulted predictions URIs.
        """

        request_body_content = {
            self.__TRAIN_FIELD: train_dataset_name,
            self.__TEST_FIELD: test_dataset_name,
            self.__CODE_FIELD: modeling_code,
            self.__CLASSIFIERS_LIST_FIELD: model_classifiers,
        }
        response = requests.post(url=self.__service_url,
                                 json=request_body_content)

        return self.__response_treat.treatment(response, pretty_response)

    def search_all_builders(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all model predictions metadata, it
        does not retrieve the model predictions content.

        pretty_response: If true return indented string, else return dict.

        return: A list with all model predictions metadata stored in Learning
        Orchestra or an empty result.
        """

        response = self.__entity_reader.read_all_instances_from_entity()

        return self.__response_treat.treatment(response, pretty_response)

    def search_builder_register_predictions(self,
                                            builder_name: str,
                                            query: dict = {},
                                            limit: int = 10,
                                            skip: int = 0,
                                            pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for retrieving the model
        predictions content.

        pretty_response: If true return indented string, else return dict.
        builder_name: Represents the model predictions name.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: A page with some tuples or registers inside or an error if there
        is no such projection. The current page is also returned to be used in
        future content requests.
        """

        response = self.__entity_reader.read_entity_content(
            builder_name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def search_builder(self, builder_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving a specific
        model prediction metadata.

        pretty_response: If true return indented string, else return dict.
        builder_name: Represents the model predictions name.
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: Specific model prediction metadata stored in Learning Orchestra
        or an error if there is no such projections.
        """
        response = self.search_builder_register_predictions(
            builder_name, limit=1,
            pretty_response=pretty_response)

        return response

    def delete_builder(self, builder_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting a model prediction.
        The delete operation is always synchronous because it is very fast,
        since the deletion is performed in background.

        pretty_response: If true return indented string, else return dict.
        builder_name: Represents the projection name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        cluster_url_dataset = f'{self.__service_url}/{builder_name}'

        response = requests.delete(cluster_url_dataset)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, dataset_name: str) -> dict:
        return self.__observer.wait(dataset_name)
