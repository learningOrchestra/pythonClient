import requests
import time
from response_treat import ResponseTreat
from dataset.dataset import Dataset


class Builder:
    """

    """

    def __init__(self, ip_from_cluster):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/builder"
        self.response_treat = ResponseTreat()
        self.TRAIN_NAME = "trainDatasetName"
        self.TEST_NAME = "testDatasetName"
        self.CODE = "modelingCode"
        self.CLASSIFIERS_LIST = "classifiersList"
        self.dataset = Dataset(ip_from_cluster)
        self.WAIT_TIME = 3
        self.METADATA_INDEX = 0

    def run_builder_async(self, train_dataset_name, test_dataset_name,
                          preprocessor_code, model_classifier,
                          pretty_response=False):
        """

        """
        if pretty_response:
            print(
                "\n----------"
                + " CREATE MODEL WITH "
                + train_dataset_name
                + " AND "
                + test_dataset_name
                + " ----------"
            )
        self.dataset.verify_dataset_processing_done(train_dataset_name,
                                                    pretty_response)
        self.dataset.verify_dataset_processing_done(test_dataset_name,
                                                    pretty_response)
        request_body_content = {
            self.TRAIN_NAME: train_dataset_name,
            self.TEST_NAME: test_dataset_name,
            self.CODE: preprocessor_code,
            self.CLASSIFIERS_LIST: model_classifier,
        }
        response = requests.post(url=self.cluster_url,
                                 json=request_body_content)

        return self.response_treat.treatment(response, pretty_response)

    def run_builder_sync(self, train_dataset_name, test_dataset_name,
                         preprocessor_code, model_classifier,
                         pretty_response=False):
        """

        """
        if pretty_response:
            print(
                "\n----------"
                + " CREATE MODEL WITH "
                + train_dataset_name
                + " AND "
                + test_dataset_name
                + " ----------"
            )
        self.dataset.verify_dataset_processing_done(train_dataset_name,
                                                    pretty_response)
        self.dataset.verify_dataset_processing_done(test_dataset_name,
                                                    pretty_response)
        request_body_content = {
            self.TRAIN_NAME: train_dataset_name,
            self.TEST_NAME: test_dataset_name,
            self.CODE: preprocessor_code,
            self.CLASSIFIERS_LIST: model_classifier,
        }
        response = requests.post(url=self.cluster_url,
                                 json=request_body_content)

        self.verify_model_processing_done(train_dataset_name,
                                          pretty_response=pretty_response)
        self.verify_model_processing_done(train_dataset_name,
                                          pretty_response=pretty_response)

        return self.response_treat.treatment(response, pretty_response)

    def search_all_model(self, pretty_response=False):
        """

        """
        cluster_url_tsne = self.cluster_url
        response = requests.get(cluster_url_tsne)
        return self.response_treat.treatment(response, pretty_response)

    def search_model_prediction(self, model_name, query={}, limit=10, skip=0,
                                pretty_response=False):
        """

        """
        cluster_url_dataset = self.cluster_url + "/" + model_name + \
                              "?query=" + str(query) + \
                              "&limit=" + str(limit) + \
                              "&skip=" + str(skip)
        response = requests.get(cluster_url_dataset)
        return self.response_treat.treatment(response, pretty_response)

    def search_model(self, model_name, pretty_response=False):
        """

        """
        response = self.search_model_prediction(model_name, limit=1,
                                                pretty_response=pretty_response)
        return response

    def verify_model_processing_done(self, model_name,
                                     pretty_response=False):
        """

        """
        if pretty_response:
            print("\n---------- WAITING " + model_name + " FINISH ----------")
        while True:
            time.sleep(self.WAIT_TIME)
            response = self.search_model(model_name,
                                         pretty_response=False)
            if len(response["result"]) == 0:
                continue
            if response["result"][self.METADATA_INDEX]["finished"]:
                break

    def delete_model(self, model_name, pretty_response=False):
        """

        """
        cluster_url_dataset = self.cluster_url + "/" + model_name
        response = requests.delete(cluster_url_dataset)
        return self.response_treat.treatment(response, pretty_response)
