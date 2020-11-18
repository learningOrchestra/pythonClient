import requests
import time
from response_treat import ResponseTreat
from dataset.dataset import Dataset
from PIL import Image


class Pca:
    def __init__(self, ip_from_cluster):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/explore/pca"
        self.response_treat = ResponseTreat()
        self.INPUT_NAME = "inputDatasetName"
        self.OUTPUT_NAME = "outputPlotName"
        self.LABEL = "label"
        self.dataset = Dataset(ip_from_cluster)
        self.WAIT_TIME = 3
        self.METADATA_INDEX = 0

    def run_pca_sync(self, dataset_name, pca_name, label,
                     pretty_response=False):
        """
        description: This method run PCA algorithm to create an image plot
        synchronously, the caller waits until the PCA image is inserted into
        the Learning Orchestra storage mechanism.

        dataset_name: Name of dataset.
        pca_name: Name of PCA image plot.
        label: The label is the label name of the column for machine learning
        datasets which has labeled tuples.
        pretty_response: If true open an image plot, else return link to open
        image plot.

        return: A JSON object with error or warning messages. In case of
        success, it returns a PCA image plot.
        """
        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: pca_name,
            self.LABEL: label,
        }
        self.dataset.verify_dataset_processing_done(dataset_name,
                                                    pretty_response)
        request_url = self.cluster_url
        response = requests.post(url=request_url, json=request_body)
        self.verify_pca_processing_done(pca_name,
                                        pretty_response=pretty_response)
        if pretty_response:
            print(
                "\n----------"
                + " CREATE PCA IMAGE PLOT FROM "
                + dataset_name
                + " TO "
                + pca_name
                + " ----------"
            )
        return self.response_treat.treatment(response, pretty_response)

    def run_pca_async(self, dataset_name, pca_name, label,
                      pretty_response=False):
        """
        description: This method run PCA algorithm to create an image plot
        asynchronously, the caller does not wait until the PCA image is
        inserted into the Learning Orchestra storage mechanism. Instead,
        the caller receives a JSON object with a URL to proceed future calls
        to verify if the PCA image was inserted.

        dataset_name: Name of dataset.
        pca_name: Name of PCA image plot.
        label: The label is the label name of the column for machine learning
        datasets which has labeled tuples.
        pretty_response: If true open an image plot, else return link to open
        image plot.

        return: A JSON object with error or warning messages. In case of
        success, it returns a PCA image plot.
        """
        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: pca_name,
            self.LABEL: label,
        }
        self.dataset.verify_dataset_processing_done(dataset_name,
                                                    pretty_response)
        request_url = self.cluster_url
        response = requests.post(url=request_url, json=request_body)
        if pretty_response:
            print(
                "\n----------"
                + " CREATE PCA IMAGE PLOT FROM "
                + dataset_name
                + " TO "
                + pca_name
                + " ----------"
            )
        return self.response_treat.treatment(response, pretty_response)

    def search_all_pca(self, pretty_response=False):
        """
        description: This method retrieves all PCAs plot names, it does not
        retrieve the image plot.

        pretty_response: If true return indented string, else return dict.

        return: A list with all PCAs plot names stored in Learning Orchestra
        or an empty result.
        """
        cluster_url_pca = self.cluster_url
        response = requests.get(cluster_url_pca)
        return self.response_treat.treatment(response, pretty_response)

    def search_pca_plot(self, pca_name, pretty_response=False):
        """
        description: This method retrieves a PCA image plot.

        pca_name: Name of PCA image plot.
        pretty_response: If true open an image plot, else return link to
        open image plot.

        return: A link to an image plot or open an image plot.
        """
        if pretty_response:
            print(
                "\n----------"
                + " READ"
                + pca_name
                + " PCA IMAGE PLOT "
                + " ----------"
            )
        cluster_url_pca = self.cluster_url + "/" + pca_name
        if pretty_response:
            img = Image.open(requests.get(cluster_url_pca, stream=True).raw)
            img.show()
        else:
            return cluster_url_pca

    def delete_pca_plot(self, pca_name, pretty_response=False):
        """
        description: This method is responsible for deleting the PCA image plot.
        The delete operation is always synchronous because it is very fast,
        since the deletion is performed in background.

        pretty_response: If true return indented string, else return dict.
        pca_name: Represents the PCA name.

        return: JSON object with an error message, a warning message or a
        correct delete message.
        """
        cluster_url_pca = self.cluster_url + "/" + pca_name
        response = requests.delete(cluster_url_pca)
        return self.response_treat.treatment(response, pretty_response)

    def verify_pca_exist(self, pca_name):
        """
        description: This method is responsible to verify if a PCA image
        exist into the Learning Orchestra storage mechanism.

        pca_name: Name of PCA image plot.

        return: True if the PCA requested exist, false if does not.
        """
        all_pca = self.search_all_pca()
        pca_name += ".png"
        return pca_name in all_pca.get('result')

    def verify_pca_processing_done(self, pca_name,
                                   pretty_response=False):
        """
        description: This method check from time to time using Time lib, if a
        projection has finished being inserted into the Learning Orchestra
        storage mechanism.

        pca_name: Name of PCA image plot.
        pretty_response: If true return indented string, else return dict.
        """
        if pretty_response:
            print(
                "\n---------- WAITING " + pca_name + " FINISH -------"
                                                     "---")
        while True:
            time.sleep(self.WAIT_TIME)
            response = self.search_pca(pca_name)
            if response:
                break
            continue