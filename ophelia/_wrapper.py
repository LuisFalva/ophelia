from ._logger import OpheliaLogger


class DataFrameWrapper:

    def __init__(self, SparkDataFrameClass):
        self.__logger = OpheliaLogger()
        self.SparkDataFrameWrapper = SparkDataFrameClass
        self.metadata = None

    def __call__(self, wrap_object):
        self.SparkDataFrameWrapper.wrap_object = wrap_object
        self.__logger.info(f"Ophelia Wrapped '{wrap_object}' Function")

    def _metadata(self, meta_dict):
        self.metadata = meta_dict
        self.__logger.info(f"Ophelia Metadata Tracker Dict '{meta_dict}'")
