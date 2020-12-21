from .observer import Observer
from .response_treat import ResponseTreat
from .dataset import Dataset
import requests
from typing import Union


class Builder:
    def __init__(self, ip_from_cluster: str):
        self.CLUSTER_IP = ip_from_cluster
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/builder"
        self.response_treat = ResponseTreat()
        self.TRAIN_NAME = "trainDatasetName"
        self.TEST_NAME = "testDatasetName"
        self.CODE = "modelingCode"
        self.CLASSIFIERS_LIST = "classifiersList"
        self.dataset = Dataset(ip_from_cluster)
        self.METADATA_INDEX = 0

    def run_builder_sync(self,
                         train_dataset_name: str,
                         test_dataset_name: str,
                         modeling_code: str,
                         model_classifiers: list,
                         pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method resource join several steps of machine
        learning workflow (transform, tune, train and evaluate) coupling in
        a unique resource, builder creates several model predictions using
        your own modeling code using a defined set of classifiers. This is made
        synchronously, the caller waits until the model predictions are inserted
        into the Learning Orchestra storage mechanism.

        train_dataset_name: Represent final train dataset.
        test_dataset_name: Represent final test dataset.
        modeling_code: Represent Python3 code for pyspark preprocessing model
        model_classifiers: list of initial classifiers to be used in model
        pretty_response: returns indented string for visualization if True

        return: The resulted predictions URIs.
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

        request_body_content = {
            self.TRAIN_NAME: train_dataset_name,
            self.TEST_NAME: test_dataset_name,
            self.CODE: modeling_code,
            self.CLASSIFIERS_LIST: model_classifiers,
        }
        response = requests.post(url=self.cluster_url,
                                 json=request_body_content)

        observer = Observer(test_dataset_name, self.CLUSTER_IP)

        for model in model_classifiers:
            observer.set_dataset_name(test_dataset_name + model)
            observer.observe_processing(pretty_response)

        return self.response_treat.treatment(response, pretty_response)

    def run_builder_async(self,
                          train_dataset_name: str,
                          test_dataset_name: str,
                          modeling_code: str,
                          model_classifiers: list,
                          pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method resource join several steps of machine
        learning workflow (transform, tune, train and evaluate) coupling in
        a unique resource, builder creates several model predictions using
        your own modeling code using a defined set of classifiers. This is made
        asynchronously, the caller does not wait until the model predictions are
        inserted into the Learning Orchestra storage mechanism. Instead, the
        caller receives a JSON object with a URL to proceed future calls to
        verify if the model predictions are inserted.

        train_dataset_name: Represent final train dataset.
        test_dataset_name: Represent final test dataset.
        modeling_code: Represent Python3 code for pyspark preprocessing model
        model_classifiers: list of initial classifiers to be used in model
        pretty_response: returns indented string for visualization if True

        return: The resulted predictions URIs.
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

        request_body_content = {
            self.TRAIN_NAME: train_dataset_name,
            self.TEST_NAME: test_dataset_name,
            self.CODE: modeling_code,
            self.CLASSIFIERS_LIST: model_classifiers,
        }
        response = requests.post(url=self.cluster_url,
                                 json=request_body_content)

        return self.response_treat.treatment(response, pretty_response)

    def search_all_model(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all model predictions metadata, it
        does not retrieve the model predictions content.

        pretty_response: If true return indented string, else return dict.

        return: A list with all model predictions metadata stored in Learning
        Orchestra or an empty result.
        """

        cluster_url_tsne = self.cluster_url

        response = requests.get(cluster_url_tsne)

        return self.response_treat.treatment(response, pretty_response)

    def search_model_prediction(self,
                                model_name: str,
                                query: dict = {},
                                limit: int = 10,
                                skip: int = 0,
                                pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for retrieving the model
        predictions content.

        pretty_response: If true return indented string, else return dict.
        model_name: Represents the model predictions name.
        query: Query to make in MongoDB(default: empty query)
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: A page with some tuples or registers inside or an error if there
        is no such projection. The current page is also returned to be used in
        future content requests.
        """

        cluster_url_dataset = self.cluster_url + "/" + model_name + \
                              "?query=" + str(query) + \
                              "&limit=" + str(limit) + \
                              "&skip=" + str(skip)

        response = requests.get(cluster_url_dataset)

        return self.response_treat.treatment(response, pretty_response)

    def search_model(self, model_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description:  This method is responsible for retrieving a specific
        model prediction metadata.

        pretty_response: If true return indented string, else return dict.
        model_name: Represents the model predictions name.
        limit: Number of rows to return in pagination(default: 10) (maximum is
        set at 20 rows per request)
        skip: Number of rows to skip in pagination(default: 0)

        return: Specific model prediction metadata stored in Learning Orchestra
        or an error if there is no such projections.
        """
        response = self.search_model_prediction(model_name, limit=1,
                                                pretty_response=pretty_response)

        return response

    def delete_model(self, model_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting a model prediction.
        The delete operation is always synchronous because it is very fast,
        since the deletion is performed in background.

        pretty_response: If true return indented string, else return dict.
        model_name: Represents the projection name.

        return: JSON object with an error message, a warning message or a
        correct delete message
        """

        cluster_url_dataset = self.cluster_url + "/" + model_name

        response = requests.delete(cluster_url_dataset)

        return self.response_treat.treatment(response, pretty_response)
