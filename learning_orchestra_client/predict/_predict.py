from learning_orchestra_client.observe.observe import Observer
from learning_orchestra_client._util._response_treat import ResponseTreat
from learning_orchestra_client._util._entity_reader import EntityReader
import requests
from typing import Union


class Predict:
    __MODEL_NAME_FIELD = "modelName"
    __PARENT_NAME_FIELD = "parentName"
    __METHOD_NAME_FIELD = "method"
    __ClASS_PARAMETERS_FIELD = "methodParameters"
    __NAME_FIELD = "name"
    __DESCRIPTION_FIELD = "description"

    def __init__(self, cluster_ip: str, api_path: str):
        self.__service_url = f'{cluster_ip}{api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def create_prediction_sync(self,
                               name: str,
                               model_name: str,
                               parent_name: str,
                               method_name: str,
                               parameters: dict,
                               description: str = "",
                               pretty_response: bool = False) -> \
            Union[dict, str]:
        """
        description: This method is responsible to predict models in sync mode


        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the prediction output object that will be created.
        parent_name: Is the name of the previous ML step of the pipeline
        method_name: is the name of the method to be executed (the ML tool way
        to predict models)
        parameters: Is the set of parameters used by the method

        return: A JSON object with an error or warning message or a URL
        indicating the correct operation.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__MODEL_NAME_FIELD: model_name,
            self.__PARENT_NAME_FIELD: parent_name,
            self.__METHOD_NAME_FIELD: method_name,
            self.__ClASS_PARAMETERS_FIELD: parameters,
            self.__DESCRIPTION_FIELD: description}

        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        self.__observer.wait(name)

        return self.__response_treat.treatment(response, pretty_response)

    def create_prediction_async(self,
                                name: str,
                                model_name: str,
                                parent_name: str,
                                method_name: str,
                                parameters: dict,
                                description: str = "",
                                pretty_response: bool = False) -> \
            Union[dict, str]:
        """
        description: This method is responsible to predict models in async mode.
        A wait method call is mandatory due to the asynchronous aspect.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the prediction output object that will be created.
        parent_name: Is the name of the previous ML step of the pipeline
        method_name: is the name of the method to be executed (the ML tool way
        to predict models)
        parameters: Is the set of parameters used by the method

        return: A JSON object with an error or warning message or a URL
        indicating the correct operation.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__MODEL_NAME_FIELD: model_name,
            self.__PARENT_NAME_FIELD: parent_name,
            self.__METHOD_NAME_FIELD: method_name,
            self.__ClASS_PARAMETERS_FIELD: parameters,
            self.__DESCRIPTION_FIELD: description}

        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        return self.__response_treat.treatment(response, pretty_response)

    def search_all_predictions(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all predictions metadata, i.e., it
        does not retrieve the prediction content.

        pretty_response: If true it returns a string, otherwise a dictionary.

        return: All predict metadata stored in Learning Orchestra or an empty
        result.
        """
        response = self.__entity_reader.read_all_instances_from_entity()
        return self.__response_treat.treatment(response, pretty_response)

    def delete_prediction(self, name: str, pretty_response=False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting the prediction.
        This delete operation is asynchronous, so it does not lock the caller
         until the deletion finished. Instead, it returns a JSON object with a
         URL for a future use. The caller uses the URL for delete checks.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Represents the prediction name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """
        request_url = f'{self.__service_url}/{name}'

        response = requests.delete(request_url)
        return self.__response_treat.treatment(response, pretty_response)

    def search_prediction_content(self,
                                  name: str,
                                  query: dict = {},
                                  limit: int = 10,
                                  skip: int = 0,
                                  pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving all the
        prediction tuples or registers, as well as the metadata content

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the prediction object
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: A page with some predictions inside or an error if there
        is no such prediction object. The current page is also returned to be
        used in future content requests.
        """
        response = self.__entity_reader.read_entity_content(
            name, query, limit, skip)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the create_prediction_async method, delete_prediction
           method.

           name: Represents the prediction name.
           timeout: Represents the time in seconds to wait for a prediction to
           finish its run.

           return: JSON object with an error message, a warning message or a
           correct prediction result
        """
        return self.__observer.wait(name, timeout)
