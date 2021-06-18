from learning_orchestra_client.observe.observe import Observer
from learning_orchestra_client._util._response_treat import ResponseTreat
from learning_orchestra_client._util._entity_reader import EntityReader
import requests
from typing import Union


class Model:
    __CLASS_FIELD = "class"
    __MODULE_PATH_FIELD = "modulePath"
    __ClASS_PARAMETERS_FIELD = "classParameters"
    __NAME_FIELD = "modelName"
    __DESCRIPTION_FIELD = "description"

    def __init__(self, cluster_ip: str, api_path: str):
        self.__service_url = f'{cluster_ip}{api_path}'
        self.__response_treat = ResponseTreat()
        self.__cluster_ip = cluster_ip
        self.__entity_reader = EntityReader(self.__service_url)
        self.__observer = Observer(self.__cluster_ip)

    def create_model_sync(self,
                          name: str,
                          module_path: str,
                          class_name: str,
                          class_parameters: dict,
                          description: str = "",
                          pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method runs a model creation in sync mode

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the model that will be created.
        class_name: is the name of the class to be executed
        module_path: The name of the package of the ML tool used
        (Ex. Scikit-learn or TensorFlow)
        class_parameters: the set of parameters of the ML class defined
        previously

        return: A JSON object with an error or warning message or a URL
        indicating the correct operation.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__CLASS_FIELD: class_name,
            self.__MODULE_PATH_FIELD: module_path,
            self.__ClASS_PARAMETERS_FIELD: class_parameters,
            self.__DESCRIPTION_FIELD: description}

        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        self.__observer.wait(name)

        return self.__response_treat.treatment(response, pretty_response)

    def create_model_async(self,
                           name: str,
                           module_path: str,
                           class_name: str,
                           class_parameters: dict,
                           description: str = "",
                           pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method runs a model creation in async mode, thus it
        requires a wait method call

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the model that will be created.
        class_name: is the name of the class to be executed
        module_path: The name of the package of the ML tool used
        (Ex. Scikit-learn or TensorFlow)
        class_parameters: the set of parameters of the ML class defined
        previously

        return: A JSON object with an error or warning message or a URL
        indicating the future correct operation.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__CLASS_FIELD: class_name,
            self.__MODULE_PATH_FIELD: module_path,
            self.__ClASS_PARAMETERS_FIELD: class_parameters,
            self.__DESCRIPTION_FIELD: description}

        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)

        return self.__response_treat.treatment(response, pretty_response)

    def search_all_models(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all models metadata, i.e., it does
        not retrieve the model content.

        pretty_response: If true it returns a string, otherwise a dictionary.

        return: All models metadata stored in Learning Orchestra or an empty
        result.
        """
        response = self.__entity_reader.read_all_instances_from_entity()
        return self.__response_treat.treatment(response, pretty_response)

    def delete_model(self, name: str, pretty_response=False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting the model.
        This delete operation is asynchronous, so it does not lock the caller
         until the deletion finished. Instead, it returns a JSON object with a
         URL for a future use. The caller uses the URL for delete checks.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Represents the model name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        request_url = f'{self.__service_url}/{name}'

        response = requests.delete(request_url)
        return self.__response_treat.treatment(response, pretty_response)

    def search_model(self, name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves a model metadata, i.e., it does
        not retrieve the model content.

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the model name

        return: A model metadata stored in Learning Orchestra or an empty
        result.
        """

        response = self.__entity_reader.read_entity_content(
            name)

        return self.__response_treat.treatment(response, pretty_response)

    def wait(self, name: str, timeout: int = None) -> dict:
        """
           description: This method is responsible to create a synchronization
           barrier for the create_model_async method, delete_model method.

           name: Represents the model name.
           timeout: Represents the time in seconds to wait for a model creation
           to finish its run.

           return: JSON object with an error message, a warning message or a
           correct model result
        """
        return self.__observer.wait(name, timeout)
