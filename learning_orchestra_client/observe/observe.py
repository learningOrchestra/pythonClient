import requests
from learning_orchestra_client._util._response_treat import ResponseTreat

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

    def wait(self, name: str, timeout: int=0,
             observer_name:str='', pretty_response: bool = False) -> dict:

        """
        Observe the end of a pipe for a timeout seconds or
        until the pipe finishes its execution.

        :param name: Represents the pipe name. Any tune, train, predict service can wait its finish with a wait method call.
        :param timeout: the maximum time to wait the observed step, in seconds. If set to 0, there will be no timeout
        :param observer_name: the name of the observer (default: observer_)

        :return: Returns a dictionary with the content of a mongo collection, representing any pipe result
        """

        return self.watch(name=name,
                         timeout=timeout,
                         type="wait",
                         observer_name=observer_name,
                         pretty_response=pretty_response)

    def watch(self, name: str, timeout: int=0, type:str="wait",
             observer_name:str='',pipeline:[]=None,
             pretty_response: bool = False) -> dict:

        """
        Observe the pipe for a timeout seconds or
        until the pipe finishes its execution. It is a more complete method,
        you can use it to configure your own pipelines if you wish. For more
        simplistic uses, try the methods "wait" and "start_observing_pipe"

        :param name: the name of the pipe to be observed. A train, predict, explore, transform or any other pipe can be observed.
        :param timeout: the maximum time to wait the observed step, in seconds. If set to 0, there will be no timeout
        :param type: type of the observation, it can be "wait" to observe the end of the pipe, "observe" to observe until the pipe change it's content or "custom" if you wish to provide your own mongo pipeline
        :param observer_name: the name of the observer (default observer_)
        :param pipeline: the custom pipeline that you wish to use on the observer. It is only used if type is set to "custom"

        :return: Returns a dictionary with the content of a mongo collection, representing any pipe result
        """

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
            if response.status_code == 408:
                raise TimeoutError(response.json()['result'])

            raise Exception(response.json()['result'])

        delete_resp = requests.delete(url=url)
        return response


    def start_observing_pipe(self, name: str, timeout: int=0,
                             observer_name:str='',
                             pretty_response: bool = False) -> dict:
        """
        It waits until a pipe change its content
        (replace, insert, update and delete mongoDB collection operation
        types), so it is a bit different
        from wait method with a timeout and a finish explicit condition.

        :param name: the name of the pipe to be observed. A train, predict, explore, transform or any other pipe can be observed.
        :param timeout: the maximum time to wait the observed step, in seconds. If set to 0, there will be no timeout
        :param observer_name: the name of the observer (default observer_)

        :returns: a dictionary with the content of a mongo collection, representing any pipe result
        """

        return self.watch(name=name,
                          timeout=timeout,
                          type="observe",
                          observer_name=observer_name,
                          pretty_response=pretty_response)