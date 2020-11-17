import requests
import time
from response_treat import ResponseTreat
from dataset.dataset import Dataset
from PIL import Image


class Tsne:
    """

        """

    def __init__(self, ip_from_cluster):
        self.cluster_url = "http://" + ip_from_cluster + \
                           "/api/learningOrchestra/v1/explore/tsne"
        self.response_treat = ResponseTreat()
        self.INPUT_NAME = "inputDatasetName"
        self.OUTPUT_NAME = "outputPlotName"
        self.LABEL = "label"
        self.dataset = Dataset(ip_from_cluster)
        self.WAIT_TIME = 3
        self.METADATA_INDEX = 0

    def search_all_tsne(self, pretty_response=False):
        """
        description: This method retrieves all PCAs metadata, it does not
        retrieve the projection content.

        pretty_response: If true return indented string, else return dict.

        return: All PCAs metadata stored in Learning Orchestra or an
        empty result.
        """
        cluster_url_tsne = self.cluster_url
        response = requests.get(cluster_url_tsne)
        return self.response_treat.treatment(response, pretty_response)

    # def search_pca_data(self):

    def search_tsne_plot(self, tsne_name, pretty_response=False):
        """

        """
        if pretty_response:
            print(
                "\n----------"
                + " READ"
                + tsne_name
                + " tsne IMAGE PLOT "
                + " ----------"
            )
        cluster_url_tsne = self.cluster_url + "/" + tsne_name
        img = Image.open(requests.get(cluster_url_tsne, stream=True).raw)
        if pretty_response:
            return img.show()
        else:
            return cluster_url_tsne

    def search_tsne(self, tsne_name):
        """

        """
        all_tsne = self.search_all_tsne()
        tsne_name += ".png"
        if tsne_name in all_tsne.get('result'):
            return True
        else:
            return False

    def run_tsne_async(self, dataset_name, tsne_name, label,
                       pretty_response=False):
        """

        """
        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: tsne_name,
            self.LABEL: label,
        }
        self.dataset.verify_dataset_processing_done(dataset_name,
                                                    pretty_response)
        request_url = self.cluster_url
        response = requests.post(url=request_url, json=request_body)
        if pretty_response:
            print(
                "\n----------"
                + " CREATE tsne IMAGE PLOT FROM "
                + dataset_name
                + " TO "
                + tsne_name
                + " ----------"
            )
        return self.response_treat.treatment(response, pretty_response)

    def run_tsne_sync(self, dataset_name, tsne_name, label,
                      pretty_response=False):
        """

        """
        request_body = {
            self.INPUT_NAME: dataset_name,
            self.OUTPUT_NAME: tsne_name,
            self.LABEL: label,
        }
        self.dataset.verify_dataset_processing_done(dataset_name,
                                                    pretty_response)
        request_url = self.cluster_url
        response = requests.post(url=request_url, json=request_body)
        self.verify_tsne_processing_done(tsne_name,
                                         pretty_response=pretty_response)
        if pretty_response:
            print(
                "\n----------"
                + " CREATE PCA IMAGE PLOT FROM "
                + dataset_name
                + " TO "
                + tsne_name
                + " ----------"
            )
        return self.response_treat.treatment(response, pretty_response)

    def verify_tsne_processing_done(self, tsne_name,
                                    pretty_response=False):
        """
        description: This method check from time to time using Time lib, if a
        projection has finished being inserted into the Learning Orchestra
        storage mechanism.

        pretty_response: If true return indented string, else return dict.
        """
        if pretty_response:
            print(
                "\n---------- WAITING " + tsne_name + " FINISH -------"
                                                      "---")
        while True:
            time.sleep(self.WAIT_TIME)
            response = self.search_tsne(tsne_name)
            if not response:
                continue
            if response:
                break

    def delete_tsne_plot(self, tsne_name, pretty_response=False):
        """

        """
        cluster_url_tsne = self.cluster_url + "/" + tsne_name
        response = requests.delete(cluster_url_tsne)
        return self.response_treat.treatment(response, pretty_response)
