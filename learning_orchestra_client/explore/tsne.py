from .._response_treat import ResponseTreat
from ..dataset import Dataset
from PIL import Image
import requests
import time
from typing import Union


class Tsne:
    def __init__(self, ip_from_cluster: str):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/explore/tsne"
        self.response_treat = ResponseTreat()
        self.INPUT_NAME = "inputDatasetName"
        self.OUTPUT_NAME = "outputPlotName"
        self.LABEL = "label"
        self.dataset = Dataset(ip_from_cluster)
        self.WAIT_TIME = 5
        self.CLUSTER_IP = ip_from_cluster

    def run_tsne_sync(self,
                      dataset_name: str,
                      tsne_name: str,
                      label: str,
                      pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method run t_SNE algorithm to create an image plot
        synchronously, the caller waits until the t_SNE image is inserted into
        the Learning Orchestra storage mechanism.

        dataset_name: Name of dataset.
        tsne_name: Name of t_SNE image plot.
        label: The label is the label name of the column for machine learning
        datasets which has labeled tuples.
        pretty_response: If true open an image plot, else return link to open
        image plot.

        return: A JSON object with error or warning messages. In case of
        success, it returns a t_SNE image plot.
        """

        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: tsne_name,
            self.LABEL: label,
        }
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        self.verify_tsne_exist(tsne_name, pretty_response)

        if pretty_response:
            print(
                "\n----------"
                + " CREATE TSNE IMAGE PLOT FROM "
                + dataset_name
                + " TO "
                + tsne_name
                + " ----------"
            )
        return self.response_treat.treatment(response, pretty_response)

    def run_tsne_async(self,
                       dataset_name: str,
                       tsne_name: str,
                       label: str,
                       pretty_response: bool = False) -> Union[dict, str]:
        """
        description: This method run t_SNE algorithm to create an image plot
        asynchronously, the caller does not wait until the t_SNE image is
        inserted into the Learning Orchestra storage mechanism. Instead,
        the caller receives a JSON object with a URL to proceed future calls
        to verify if the t_SNE image was inserted.

        dataset_name: Name of dataset.
        tsne_name: Name of t_SNE image plot.
        label: The label is the label name of the column for machine learning
        datasets which has labeled tuples.
        pretty_response: If true open an image plot, else return link to open
        image plot.

        return: A JSON object with error or warning messages. In case of
        success, it returns a t_SNE image plot.
        """

        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: tsne_name,
            self.LABEL: label,
        }
        request_url = self.cluster_url

        response = requests.post(url=request_url, json=request_body)

        if pretty_response:
            print(
                "\n----------"
                + " CREATE TSNE IMAGE PLOT FROM "
                + dataset_name
                + " TO "
                + tsne_name
                + " ----------"
            )

        return self.response_treat.treatment(response, pretty_response)

    def search_all_tsne(self, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves all t_SNE plot names, it does not
        retrieve the image plot.

        pretty_response: If true return indented string, else return dict.

        return: A list with all t_SNEs plot names stored in Learning Orchestra
        or an empty result.
        """

        cluster_url_tsne = self.cluster_url

        response = requests.get(cluster_url_tsne)

        return self.response_treat.treatment(response, pretty_response)

    def search_tsne_plot(self, tsne_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method retrieves a t_SNE image plot.

        tsne_name: Name of t_SNE image plot.
        pretty_response: If true open an image plot, else return link to
        open image plot.

        return: A link to an image plot or open an image plot.
        """

        cluster_url_tsne = self.cluster_url + "/" + tsne_name

        if pretty_response:
            print(
                "\n----------"
                + " READ"
                + tsne_name
                + " tsne IMAGE PLOT "
                + " ----------"
            )
            img = Image.open(requests.get(cluster_url_tsne, stream=True).raw)
            img.show()
        else:
            return cluster_url_tsne

    def delete_tsne_plot(self, tsne_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible for deleting the t_SNE image
        plot. The delete operation is always synchronous because it is very
        fast, since the deletion is performed in background.

        pretty_response: If true return indented string, else return dict.
        tsne_name: Represents the tsne name.

        return: JSON object with an error message, a warning message or a
        correct delete message.
        """

        cluster_url_tsne = self.cluster_url + "/" + tsne_name

        response = requests.delete(cluster_url_tsne)

        return self.response_treat.treatment(response, pretty_response)

    def verify_tsne_exist(self, tsne_name: str, pretty_response: bool = False) \
            -> Union[dict, str]:
        """
        description: This method is responsible to verify if a t_SNE image
        exist into the Learning Orchestra storage mechanism.

        tsne_name: Name of t_SNE image plot.

        return: True if the t_SNE requested exist, false if does not.
        """

        if pretty_response:
            print("\n---------- CHECKING IF " + tsne_name + " FINISHED "
                                                            "----------")

        exist = False
        tsne_name += ".png"
        while not exist:
            time.sleep(self.WAIT_TIME)
            all_tsne = self.search_all_tsne()
            exist = tsne_name in all_tsne.get('result')
