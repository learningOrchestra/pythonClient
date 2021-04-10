from pymongo import MongoClient, change_stream
import logging


class Observer:
    def __init__(self, cluster_ip: str):

        cluster_ip = cluster_ip.replace("http://", "")
        mongo_url = f'mongodb://root:owl45%2321@{cluster_ip}'
        mongo_client = MongoClient(
            mongo_url
            )

        self.__database = mongo_client.database

    def wait(self, dataset_name: str, pretty_response: bool = False) -> dict:
        """
        :description: Observe the finished processing status from some
        processing, blocking the code execution until finish processing.

        :return: A dict with metadata file of used dataset name.
        """

        dataset_collection = self.__database[dataset_name]
        metadata_query = {"_id": 0}
        dataset_metadata = dataset_collection.find_one(metadata_query)

        if dataset_metadata is None:
            logging.warning("Dataset name or dataset URL invalid")
            return {}

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
        return dataset_collection.watch(
            observer_query,
            full_document='updateLookup').next()['fullDocument']

    def observe_storage(self, dataset_name: str) -> \
            change_stream.CollectionChangeStream:
        """
        :description: Get all changes from a dataset

        :return: A pymongo CollectionChangeStream object, use the builtin
        next() method to iterate over changes.
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
        return self.__database[dataset_name].watch(
            observer_query,
            full_document='updateLookup')
