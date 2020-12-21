from pymongo import MongoClient, change_stream


class Observer:
    def __init__(self, dataset_name: str, cluster_ip: str):
        self.dataset_name = dataset_name

        mongo_url = 'mongodb://root:owl45%2321@' + cluster_ip
        mongo_client = MongoClient(mongo_url)
        self.database = mongo_client['database']

    def set_dataset_name(self, dataset_name: str):
        """
        :description: Changes the dataset name used in object.
        :param dataset_name: Name of dataset to observe.

        :return: None.
        """
        self.dataset_name = dataset_name

    def get_dataset_name(self) -> str:
        """
        :description: Retrieve the dataset name used in object.

        :return: The dataset name.
        """
        return self.dataset_name

    def observe_processing(self, pretty_response: bool = False) -> dict:
        """
        :description: Observe the finished processing status from some
        processing, blocking the code execution until finish processing.

        :return: A dict with metadata file of used dataset name.
        """
        if pretty_response:
            print("\n---------- CHECKING IF " + self.dataset_name + " FINISHED "
                                                                    "----------")
        dataset_collection = self.database[self.dataset_name]
        metadata_query = {"_id": 0}
        dataset_metadata = dataset_collection.find_one(metadata_query)

        if dataset_metadata is None:
            print("Dataset name or dataset URL invalid")
            exit()

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

    def observe_storage(self) -> change_stream.CollectionChangeStream:
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
        return self.database[self.dataset_name].watch(
            observer_query,
            full_document='updateLookup')
