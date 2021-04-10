from ._model import Model


class ModelTensorflow(Model):

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/model/tensorflow"
        self.__cluster_ip = cluster_ip
        super().__init__(cluster_ip, self.__api_path)
