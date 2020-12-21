from .._response_treat import ResponseTreat
from ..dataset import Dataset
from PIL import Image
import requests
import time
from typing import Union


class Pca:
    def __init__(self, ip_from_cluster: str):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/explore/pca"
        self.response_treat = ResponseTreat()
        self.INPUT_NAME = "inputDatasetName"
        self.OUTPUT_NAME = "outputPlotName"
        self.LABEL = "label"
        self.dataset = Dataset(ip_from_cluster)
        self.WAIT_TIME = 5
        self.CLUSTER_IP = ip_from_cluster

    def run_pca_sync(self,
                     dataset_name: str,
                     pca_name: str,
                     label: str,
                     pretty_response: bool = False) -> Union[dict, str]:
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
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        self.verify_pca_exist(pca_name, pretty_response)

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

    def run_pca_async(self,
                      dataset_name: str,
                      pca_name: str,
                      label: str,
                      pretty_response: bool = False) -> Union[dict, str]:
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

    def search_all_pca(self, pretty_response: bool = False) -> Union[dict, str]:
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

    def search_pca_plot(self, pca_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves a PCA image plot.

        pca_name: Name of PCA image plot.
        pretty_response: If true open an image plot, else return link to
        open image plot.

        return: A link to an image plot or open an image plot.
        """

        cluster_url_pca = self.cluster_url + "/" + pca_name

        if pretty_response:
            print(
                "\n----------"
                + " READ "
                + pca_name
                + " PCA IMAGE PLOT "
                + " ----------"
            )
            img = Image.open(requests.get(cluster_url_pca, stream=True).raw)
            img.show()
        else:
            return cluster_url_pca

    def delete_pca_plot(self, pca_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
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

    def verify_pca_exist(self, pca_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible to verify if a PCA image
        exist into the Learning Orchestra storage mechanism.

        pca_name: Name of PCA image plot.

        return: True if the PCA requested exist, false if does not.
        """

        if pretty_response:
            print("\n---------- CHECKING IF " + pca_name + " FINISHED "
                                                           "----------")

        exist = False
        pca_name += ".png"

        while not exist:
            time.sleep(self.WAIT_TIME)
            all_pca = self.search_all_pca()
            exist = pca_name in all_pca.get('result')
