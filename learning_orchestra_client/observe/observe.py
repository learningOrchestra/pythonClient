from pymongo import MongoClient, change_stream


class Observer:
    __TIMEOUT_TIME_MULTIPLICATION = 1000
    __MAX_WAIT_TIME = 240

    def __init__(self, cluster_ip: str):
        cluster_ip = cluster_ip.replace("http://", "")
        mongo_url = f'mongodb://root:owl45%2321@{cluster_ip}'
        mongo_client = MongoClient(
            mongo_url
        )

        self.__database = mongo_client.database

    def wait(self, name: str, timeout: int = None) -> dict:
        """
        :description: Observe the end of a pipe for a timeout seconds or
        until the pipe finishes its execution.

        name: Represents the pipe name. Any tune, train, predict service can
        wait its finish with a
        wait method call.
        timeout: the maximum time to wait the observed step, in seconds.
        Default time is 4 minutes.

        :return: If True it returns a String. Otherwise, it returns
        a dictionary with the content of a mongo collection, representing
        any pipe result
        """

        dataset_collection = self.__database[name]
        metadata_query = {"_id": 0}
        dataset_metadata = dataset_collection.find_one(metadata_query)

        if dataset_metadata["finished"]:
            return dataset_metadata

        observer_query = [
            {'$match': {
                '$and':
                    [
                        {'operationType': 'update'},
                        {'fullDocument.finished': {'$eq': True}}
                    ]
            }}
        ]

        timeout = self.__MAX_WAIT_TIME if timeout is None else timeout 

        return dataset_collection.watch(
            observer_query,
            full_document='updateLookup',
            max_await_time_ms=timeout * self.__TIMEOUT_TIME_MULTIPLICATION
        ).next()['fullDocument']

    def observe_pipe(self, name: str, timeout: int = None) -> \
            change_stream.CollectionChangeStream:
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

        observer_query = [
            {'$match': {
                '$or': [
                    {'operationType': 'replace'},
                    {'operationType': 'insert'},
                    {'operationType': 'update'},
                    {'operationType': 'delete'}

                ]
            }}
        ]
        return self.__database[name].watch(
            observer_query,
            max_await_time_ms=timeout * self.__TIMEOUT_TIME_MULTIPLICATION,
            full_document='updateLookup')
