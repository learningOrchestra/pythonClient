from learning_orchestra_client.observe.observe import Observer
from learning_orchestra_client._util._response_treat import ResponseTreat
from learning_orchestra_client._util._entity_reader import EntityReader
import requests
from typing import Union


class BuilderPycaret:
    __CODE_FIELD = "function"
    __PARAMETERS_FIELD = "functionParameters"
    __NAME_FIELD = "name"
    __DESCRIPTION_FIELD = "description"
    __FILE_NAME = "filename"

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/builder/pycaret"
        self.__api_path_get_file = "/api/learningOrchestra/v1/explore/getlocaldatapycaret"
        self.__service_url_get_file = f'{cluster_ip}{self.__api_path_get_file}'
        self.__service_url = f'{cluster_ip}{self.__api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def run_pycaret_sync(self,
                         name: str,
                         parameters: dict,
                         code: str,
                         description: str = "",
                         pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method runs a python 3 code in sync mode, so it
        represents a wildcard for the data scientist. It can be used when
        train, predict, tune, explore or any other pipe must be customized. The
        function is also useful for new pipes. pretty_response: If true it
        returns a string, otherwise a dictionary.

        name: Is the name of the object stored in Learning Orchestra storage
        system (volume or mongoDB).
        url: Url to CSV file.

        return: A JSON object with an error or warning message or the correct
        operation result.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__PARAMETERS_FIELD: parameters,
            self.__CODE_FIELD: code,
            self.__DESCRIPTION_FIELD: description}

        request_url = self.__service_url
        response = requests.post(url=request_url, json=request_body)
        self.__observer.wait(name)

        return self.__response_treat.treatment(response, pretty_response)

    def run_pycaret_async(self,
                          name: str,
                          parameters: dict,
                          code: str,
                          description: str = "",
                          pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method runs a python 3 code in async mode, so it
        represents a wildcard for the data scientist. It does not lock the
        caller, so a wait method must be used. It can be used when train,
        predict, tune, explore or any other pipe must be customized. The
        function is also useful for new pipes.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the function to be called
        code: the Python code
        parameters: the parameters of the function being called

        return: A JSON object with an error or warning message or the correct
        operation result.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__PARAMETERS_FIELD: parameters,
            self.__CODE_FIELD: code,
            self.__DESCRIPTION_FIELD: description}

        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        return self.__response_treat.treatment(response, pretty_response)

    def search_all_executions(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all created functions metadata,
        i.e., it does not retrieve the function result content.

        pretty_response: If true it returns a string, otherwise a dictionary.

        return: All function executions metadata stored in Learning Orchestra
        or an empty result.
        """
        response = self.__entity_reader.read_all_instances_from_entity()
        return self.__response_treat.treatment(response, pretty_response)

    def delete_execution(self, name: str, pretty_response=False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting the function.
        This delete operation is asynchronous, so it does not lock the caller
         until the deletion finished. Instead, it returns a JSON object with a
         URL for a future use. The caller uses the URL for delete checks.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Represents the function name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        request_url = f'{self.__service_url}/{name}'

        response = requests.delete(request_url)
        return self.__response_treat.treatment(response, pretty_response)

    def search_execution_content(self,
                                 name: str,
                                 query: dict = {},
                                 limit: int = 10,
                                 skip: int = 0,
                                 pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving the function
        results, including metadata. A function is executed many times, using
        different parameters,
        thus many results are stored
        in Learning Orchestra.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the function.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return:
         A page with some function results inside or an error if there
        is no such function. The current page is also returned to be used in
        future content requests.
        """

        response = self.__entity_reader.read_entity_content(
            name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, dataset_name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the run_function_async method or delete_function method.

           name: Represents the function name.
           timeout: Represents the time in seconds to wait for a function to
           finish its run.

           return: JSON object with an error message, a warning message or a
           correct function result
        """
        return self.__observer.wait(dataset_name, timeout)

    def search_report(self,
                      file_name: str) -> str:
        request_body = {
            self.__FILE_NAME: file_name
        }

        request_url = self.__service_url_get_file

        response = requests.get(url=request_url, json=request_body)
        return f'{response.text}'


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
        description: This method call runs several steps of a machine
        learning pipeline (transform, tune, train and evaluate, for instance)
        using a model code and several classifiers. It represents a way to run
        an entire pipeline. The caller waits until the method execution ends,
        since it is a synchronous method.

        train_dataset_name: Represent final train dataset.
        test_dataset_name: Represent final test dataset.
        modeling_code: Represent Python3 code for pyspark pre-processing model
        model_classifiers: list of initial classifiers to be used in the model
        pretty_response: if True it represents a result useful for visualization

        return: The set of predictions (URIs of them).
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
        description: This method call runs several steps of a machine
        learning pipeline (transform, tune, train and evaluate, for instance)
        using a model code and several classifiers. It represents a way to run
        an entire pipeline. The caller does not wait until the method execution
        ends, since it is an asynchronous method.

        train_dataset_name: Represent final train dataset.
        test_dataset_name: Represent final test dataset.
        modeling_code: Represent Python3 code for pyspark pre-processing model
        model_classifiers: list of initial classifiers to be used in the model
        pretty_response: if True it represents a result useful for visualization

        return: the URL to retrieve the Spark pipeline result
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
        description: This method retrieves all model predictions metadata. It
        does not retrieve the model predictions content.

        pretty_response: If true it returns a string, otherwise a dictionary.

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

    def search_builder(self, builder_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving a specific
        model metadata.

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

    def wait(self, dataset_name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the run_spark_ml_async method.

           dataset_name: Represents the pipeline name.
           timeout: Represents the time in seconds to wait for a builder to
           finish its run.

           return: JSON object with an error message, a warning message or a
           correct execution of a pipeline
        """
        return self.__observer.wait(dataset_name, timeout)
