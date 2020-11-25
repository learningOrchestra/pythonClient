from pymongo import MongoClient


class Observer:
    def __init__(self, dataset_name, cluster_ip):
        self.dataset_name = dataset_name

        mongo_url = 'mongodb://root:owl45%2321@' + cluster_ip
        mongo_client = MongoClient(mongo_url)
        self.database = mongo_client['database']

    def set_dataset_name(self, dataset_name):
        """
        :description: Changes the dataset name used in object.
        :param dataset_name: Name of dataset to observe.

        :return: None.
        """
        self.dataset_name = dataset_name

    def get_dataset_name(self):
        """
        :description: Retrieve the dataset name used in object.

        :return: The dataset name.
        """
        return self.dataset_name

    def observe_processing(self):
        """
        :description: Observe the finished processing status from some
        processing, blocking the code execution until finish processing.

        :return: A dict with metadata file of used dataset name.
        """
        observer_query = [
            {'$match': {
                '$and':
                    [
                        {'operationType': 'update'},
                        {'fullDocument.finished': {'$eq': True}}
                    ]
            }}
        ]
        return self.database[self.dataset_name].watch(
            observer_query,
            full_document='updateLookup').next()

    def observe_storage(self):
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
