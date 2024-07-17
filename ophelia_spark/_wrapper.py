from pyspark.sql import DataFrame

from ._logger import OpheliaLogger


class DataFrameWrapper:

    def __init__(self):
        self.__df_class = DataFrame.__qualname__
        self.__logger = OpheliaLogger()
        self.metadata = None

    def __call__(self, wrap_object, description=False):
        class_func = wrap_object.__qualname__
        name_py_function = class_func.split(".")[1]
        exec(f"from .functions import {class_func.split('.')[0]}")
        exec(f"{self.__df_class}.{name_py_function} = {class_func}")
        if description:
            self.__logger.info(
                f"Ophelia Wrapped '{self.__df_class}.{name_py_function}' To Spark DF Class"
            )

    def _metadata(self, meta_dict):
        self.metadata = meta_dict
        self.__logger.info(f"Ophelia Metadata Tracker Dict '{meta_dict}'")
