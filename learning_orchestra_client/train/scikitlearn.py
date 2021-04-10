from ._train import Train


class TrainScikitLearn(Train):
    __PARENT_NAME_FIELD = "parentName"
    __METHOD_NAME_FIELD = "method"
    __ClASS_PARAMETERS_FIELD = "methodParameters"
    __NAME_FIELD = "name"
    __DESCRIPTION_FIELD = "description"

    def __init__(self, cluster_ip: str):
        self.__api_path = "/api/learningOrchestra/v1/train/scikitlearn"
        self.__cluster_ip = cluster_ip
        super().__init__(cluster_ip, self.__api_path)
