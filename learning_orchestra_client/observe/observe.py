import requests
from learning_orchestra_client._util._response_treat import ResponseTreat
from pymongo import MongoClient, change_stream


class Observer:
    debug = True

    __TIMEOUT_TIME_MULTIPLICATION = 1000
    __INPUT_NAME = "filename"

    __FILENAME_REQUEST_FIELD = 'filename'
    __OBSERVER_TYPE_REQUEST_FIELD = 'observe_type'
    __TIMEOUT_REQUEST_FIELD = 'timeout'
    __OBSERVER_NAME_REQUEST_FIELD = 'observer_name'
    __OBSERVER_PIPELINE_REQUEST_FIELD = 'pipeline'
    __MICROSERVICE_PORT = '5010'


    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/observer"
        self.__service_base_url = f'{cluster_ip}:{self.__MICROSERVICE_PORT}'
        self.__service_url = f'{self.__service_base_url}{self.__api_path}'
        self.cluster_ip = cluster_ip.replace("http://", "")
        self.__response_treat = ResponseTreat()

    def wait(self, name: str, timeout: int=None, type:str="wait",
             observer_name:str='',pipeline:[]=None,
             pretty_response: bool = False) -> dict:
        """
        :description: Observe the end of a pipe for a timeout seconds or
        until the pipe finishes its execution.

        name: Represents the pipe name. Any tune, train, predict service can
        wait its finish with a
        wait method call.
        timeout: the maximum time to wait the observed step, in seconds.
        type: type of the pipeline to be observed ("all", "finish")

        :return: If True it returns a String. Otherwise, it returns
        a dictionary with the content of a mongo collection, representing
        any pipe result
        """
        if timeout is None:
            timeout = 0

        if type == "all" or type == "wait" or type == '1':
            type = "wait"
        elif type == "finish" or type == "observe" or type == '2':
            type = "observe"
        else:
            raise NameError("Invalid type parameter: " + type)

        request_url = f'{self.__service_url}'
        request_body = {
            self.__FILENAME_REQUEST_FIELD: name,
            self.__OBSERVER_TYPE_REQUEST_FIELD: type,
            self.__TIMEOUT_REQUEST_FIELD: timeout,
            self.__OBSERVER_NAME_REQUEST_FIELD: observer_name,
            self.__OBSERVER_PIPELINE_REQUEST_FIELD: pipeline
        }

        observer_uri = requests.post(url=f'{request_url}',
                                     json=request_body)

        if(observer_uri.status_code >= 200 and observer_uri.status_code < 400):
            url = f"{self.__service_base_url}{observer_uri.json()['result']}"

            response = requests.get(url=url)
        else:
            raise Exception(observer_uri.json()['result'])

        if response.status_code >= 200 and response.status_code < 400:
            response = self.__response_treat.treatment(response,pretty_response)
        else:
            raise Exception(response.json()['result'])

        delete_resp = requests.delete(url=url)
        return response


    def start_observing_pipe(self, name: str, timeout: int=0,
                             observer_name:str='',pipeline:[]=None,
                             pretty_response: bool = False) -> dict:
        """
        :description: It waits until a pipe change its content
        (replace, insert, update and delete mongoDB collection operation
        types), so it is a bit different
        from wait method with a timeout and a finish explicit condition.

        :name: the name of the pipe to be observed. A train, predict, explore,
        transform or any
        other pipe can be observed.
        timeout: the maximum time to wait the observed step, in milliseconds.

        :return: A pymongo CollectionChangeStream object. You must use the
        builtin next() method to iterate over changes.
        """

        return self.wait(name, timeout, "observe", observer_name, pipeline,
                         pretty_response)

    def stop_observing_pipe(self,
                            name: str,
                            pretty_response: bool = False) -> dict:

        request_url = f'{self.__service_url}/{name}'
        response = requests.delete(url=request_url)
        return self.__response_treat.treatment(response, pretty_response)