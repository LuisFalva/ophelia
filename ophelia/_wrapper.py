from ._logger import OpheliaLogger


class DataFrameWrapper:

    def __init__(self, SparkDataFrameClass):
        self.__df_class = SparkDataFrameClass.__qualname__
        self.__logger = OpheliaLogger()
        self.metadata = None

    def __call__(self, wrap_object):
        class_func = wrap_object.__qualname__
        name_py_function = class_func.split('.')[1]
        exec(f"{self.__df_class}.{name_py_function} = {class_func}")
        self.__logger.info(f"Ophelia Wrapped '{wrap_object}' Class Function")

    def _metadata(self, meta_dict):
        self.metadata = meta_dict
        self.__logger.info(f"Ophelia Metadata Tracker Dict '{meta_dict}'")
