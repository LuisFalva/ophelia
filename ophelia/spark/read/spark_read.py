import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from spark.utils.logger import OpheliaLogger

__all__ = ["Read", "SparkReadWrapper"]


class FormatRead:

    def __init__(self):
        self.parquet = "parquet"
        self.excel = "excel"
        self.csv = "csv"
        self.json = "json"
        self.all = [self.parquet, self.excel, self.csv, self.json]


class Read:

    __logger = OpheliaLogger()
    __format = FormatRead()

    @staticmethod
    def build_params(params) -> dict:
        path = params.getString('config.input.raw')
        stage = params.getString('config.stage')
        date = params.getString('config.target_date')
        source = params.getString('config.source')
        pivot = params.getString('config.pivot')
        return {"path": path, "stage": stage, "date": date, "source": source, "pivot": pivot}

    @staticmethod
    def read_csv(self, path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        """
        Read csv function helps to read csv format file in spark
        :param self: sparkSession instance
        :param path: str, root path from file to load
        :param header: bool, True for header config
        :param infer_schema: bool, True for infer schema type
        :return: spark DataFrame
        """
        Read.__logger.info(f"Read CSV File From Path: {path}")
        return self.read.csv(path, header=header, inferSchema=infer_schema)

    @staticmethod
    def read_parquet(self, path: str) -> DataFrame:
        """
        Read parquet function helps to read parquet format file in spark
        :param self: sparkSession instance
        :param path: str, root path from file to load
        :return: spark DataFrame
        """
        Read.__logger.info(f"Read Parquet File From Path: {path}")
        return self.read.parquet(path)

    @staticmethod
    def read_excel(self, path: str, sheet_name: str) -> DataFrame:
        """
        Read excel function helps to read excel format file in spark
        :param self: sparkSession instance
        :param path: str, root path from file to load
        :param sheet_name: str, name from excel sheet
        :return: spark DataFrame
        """
        Read.__logger.info(f"Read Excel File From Path: {path}")
        return self.createDataFrame(pd.read_excel(path, sheet_name))

    @staticmethod
    def read_json(self, path: str) -> DataFrame:
        """
        Read json function helps to read json format file in spark
        :param self: sparkSession instance
        :param path: str, root path from file to load
        :return: spark DataFrame
        """
        Read.__logger.info(f"Read Json File From Path: {path}")
        return self.read.json(path)

    @staticmethod
    def read_file(self, path_source: str, source: str, sheet: str = None,
                  header: bool = True, infer_schema: bool = True) -> DataFrame:
        """
        Read file function helps to read multi-format file in spark
        :param self: spark session instance
        :param path_source: str, root path from file to load
        :param source: str, source type format file
        :param sheet: str, name from excel sheet
        :param header: str, name from excel sheet
        :param infer_schema: str, name from excel sheet
        :return: spark DataFrame
        """
        if path_source is None or source is None:
            Read.__logger.error("'path' And 'source' Params Must Be Not None")
            raise ValueError("Params Must Be Not None")
        if source not in Read.__format.all:
            Read.__logger.error(f"Format Type '{source}' Not Available In Spark")
            raise TypeError("'source' Type Must Be: {'parquet', 'excel', 'csv', 'json'}")
        read_object = {
            Read.__format.parquet: None if source != "parquet" else Read.read_parquet(self, path_source),
            Read.__format.csv: None if source != "csv" else Read.read_csv(self, path_source, header, infer_schema),
            Read.__format.excel: None if source != "excel" else Read.read_excel(self, path_source, sheet),
            Read.__format.json: None if source != "json" else Read.read_json(self, path_source)
        }
        return read_object[source]


class SparkReadWrapper(object):

    SparkSession.readFile = Read.read_file
