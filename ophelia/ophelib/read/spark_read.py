from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import date_format, to_date, col
from ophelia.ophelib.utils.logger import OpheliaLogger
from ophelia.ophelib.read import FormatRead


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
    def read_csv(spark: SparkSession, path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        """
        Read csv function helps to read csv format file in spark
        :param spark: sparkSession instance
        :param path: str, root path from file to load
        :param header: bool, True for header config
        :param infer_schema: bool, True for infer schema type
        :return: spark DataFrame
        """
        Read.__logger.info("Read CSV File")
        csv_file = spark.read.csv(path, header=header, inferSchema=infer_schema)
        Read.__logger.info("Read CSV Successfully From Path: " + path)
        return csv_file

    @staticmethod
    def read_parquet(spark: SparkSession, path: str) -> DataFrame:
        """
        Read parquet function helps to read parquet format file in spark
        :param spark: sparkSession instance
        :param path: str, root path from file to load
        :return: spark DataFrame
        """
        Read.__logger.info("Read Parquet File")
        parquet_file = spark.read.parquet(path)
        Read.__logger.info("Read Parquet Successfully From Path: " + path)
        return parquet_file

    @staticmethod
    def read_excel(spark: SparkSession, path: str, source: str, header: str = "true") -> DataFrame:
        """
        Read excel function helps to read excel format file in spark
        :param spark: sparkSession instance
        :param path: str, root path from file to load
        :param source: str, source type format file
        :param header: str, 'true' for header config
        :return: spark DataFrame
        """
        Read.__logger.info("Read Excel File")
        excel_file = spark.read.format(source).options(header=header).load(path)
        Read.__logger.info("Read Excel Successfully From Path: " + path)
        return excel_file

    @staticmethod
    def read_json(spark: SparkSession, path: str) -> DataFrame:
        """
        Read json function helps to read json format file in spark
        :param spark: sparkSession instance
        :param path: str, root path from file to load
        :return: spark DataFrame
        """
        Read.__logger.info("Read Json File")
        excel_file = spark.read.json(path)
        Read.__logger.info("Read Json Successfully From Path: " + path)
        return excel_file

    @staticmethod
    def read_file(spark_session: SparkSession, path_source: str, source: str) -> DataFrame:
        """
        Read file function helps to read multi-format file in spark
        :param spark_session: spark session instance
        :param path_source: str, root path from file to load
        :param source: str, source type format file
        :return: spark DataFrame
        """
        if path_source is None or source is None or spark_session is None:
            Read.__logger.error("read file need 'path', 'source' and 'spark' arguments must not be None")
            raise TypeError("params must not be None")
        if source not in Read.__format.all:
            Read.__logger.error("reader type '" + source + "' not available in spark")
            raise ValueError("'source' must be in list: {'parquet', 'excel', 'csv', 'json'}")
        read_object = {
            Read.__format.parquet: None if source != "parquet" else Read.read_parquet(spark_session, path_source),
            Read.__format.csv: None if source != "csv" else Read.read_csv(spark_session, path_source),
            Read.__format.excel: None if source != "excel" else Read.read_excel(spark_session, path_source, source),
            Read.__format.json: None if source != "json" else Read.read_json(spark_session, path_source)
        }
        df = read_object[source]
        return df
    
    @staticmethod
    def build_portfolio_schema(df: DataFrame, date_col: str) -> DataFrame:
        """
        Build portfolio schema helps to generate schema data type for fund portfolio price
        :param df: spark DataFrame with fund price daily
        :param date_col: str name for DataFrame date column
        """
        if df is None or date_col is None:
            Read.__logger.error("build portfolio schema need 'df' and 'date col' not be None")
            raise TypeError("params must not be None")
        Read.__logger.info("Build Schema File")
        build_date = date_format(to_date(col(df.columns[0]), 'dd/MM/yyyy'), "yyyy-MM-dd")
        schema_portfolio = [build_date.cast("date").alias(date_col)] + [col(x).cast('float') for x in df.columns[1:]]
        schema_df = df.where(col(df.columns[0]).isNotNull()).select(schema_portfolio)
        Read.__logger.info("Build Schema Success")
        return schema_df
