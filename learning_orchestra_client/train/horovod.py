from ._train import Train
import requests
from typing import Union


class TrainHorovod(Train):
    __PARENT_NAME_FIELD = "parentName"
    __METHOD_NAME_FIELD = "method"
    __ClASS_PARAMETERS_FIELD = "methodParameters"
    __NAME_FIELD = "name"
    __DESCRIPTION_FIELD = "description"
    __COMPILE_CODE = "compileCode"
    __MONITORING_PATH = "monitoringPath"

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/train/horovod"
        self.__cluster_ip = cluster_ip
        super().__init__(cluster_ip, self.__api_path)

    def create_training_async(self,
                              name: str,
                              model_name: str,
                              parent_name: str,
                              parameters: dict,
                              description: str = "",
                              compiling_code: str = "",
                              monitoring_path: str = None,
                              pretty_response: bool = False) -> \
            Union[dict, str]:
        """
                description: This method is responsible to train models in async mode.
                A wait method call is mandatory due to the asynchronous aspect.

                pretty_response: If true it returns a string, otherwise a dictionary.
                name: Is the name of the train output object that will be created.
                parent_name: Is the name of the previous ML step of the pipeline
                method_name: is the name of the method to be executed (the ML tool way
                to train models)
                parameters: Is the set of parameters used by the method

                return: A JSON object with an error or warning message or a URL
                indicating the correct operation.
                """
        request_body = {
            self.__NAME_FIELD: name,
            self.__MODEL_NAME_FIELD: model_name,
            self.__PARENT_NAME_FIELD: parent_name,
            self.__ClASS_PARAMETERS_FIELD: parameters,
            self.__DESCRIPTION_FIELD: description,
            self.__COMPILE_CODE: compiling_code,
            self.__MONITORING_PATH: monitoring_path,
        }

        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        return self.__response_treat.treatment(response, pretty_response)

    def create_training_sync(self,
                             name: str,
                             model_name: str,
                             parent_name: str,
                             parameters: dict,
                             description: str = "",
                             compiling_code: str = "",
                             monitoring_path: str = None,
                             pretty_response: bool = False) -> \
            Union[dict, str]:
        """
        description: This method is responsible to train models in sync mode

        pretty_response: If true it returns a string, otherwise a dictionary.
        name: Is the name of the train output object that will be created.
        parent_name: Is the name of the previous ML step of the pipeline
        method_name: is the name of the method to be executed (the ML tool way
        to train models)
        parameters: Is the set of parameters used by the method

        return: A JSON object with an error or warning message or a URL
        indicating the correct operation.
        """
        request_body = {
            self.__NAME_FIELD: name,
            self.__MODEL_NAME_FIELD: model_name,
            self.__PARENT_NAME_FIELD: parent_name,
            self.__ClASS_PARAMETERS_FIELD: parameters,
            self.__DESCRIPTION_FIELD: description,
            self.__COMPILE_CODE: compiling_code,
            self.__MONITORING_PATH: monitoring_path,
        }

        request_url = self.__service_url

        response = requests.post(url=request_url, json=request_body)
        self.__observer.wait(name)

        return self.__response_treat.treatment(response, pretty_response)
