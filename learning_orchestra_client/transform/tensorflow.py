from ._transform import Transform


class TransformTensorflow(Transform):
    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/transform/tensorflow"
        self.__cluster_ip = cluster_ip
        super().__init__(cluster_ip, self.__api_path)
