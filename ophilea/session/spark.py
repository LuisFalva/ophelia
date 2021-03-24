from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from .._logger import OphileaLogger


class OpheliaSpark:

    __logger = OphileaLogger()

    def __init__(self):
        self.ophelia_spark = None

    def __build_spark_app(self, app_name):
        self.ophelia_spark = SparkSession.builder.appName(app_name).getOrCreate()

    def __build_spark(self):
        self.ophelia_spark = SparkSession.builder.appName("No 'appName' configured").getOrCreate()

    def build_spark_session(self, app_name: str = None) -> SparkSession:
        """
        Build spark session is the builder method to initialize spark session under the hood of Ophelia
        :param app_name: str, app name description for spark job name
        :return: spark session
        """
        if not app_name:
            OpheliaSpark.__logger.info("Build Spark Session")
            self.__build_spark()
            OpheliaSpark.__logger.info("Spark Version: " + self.ophelia_spark.version)
            OpheliaSpark.__logger.warning("Please, Be Aware To Set App Name Next Time...")
            return self.ophelia_spark
        else:
            OpheliaSpark.__logger.warning("Initializing Spark Session")
            self.__build_spark_app(app_name=app_name)
            OpheliaSpark.__logger.info("Spark Version: " + self.ophelia_spark.version)
            OpheliaSpark.__logger.info("This Is: '" + self.ophelia_spark.sparkContext.appName + "' App")
            return self.ophelia_spark

    def build_spark_context(self) -> SparkContext:
        """
        Build spark context is the builder method to initialize spark context given a spark session
        :return: spark context
        """
        OpheliaSpark.__logger.info("Spark Context Initialized Success")
        return self.ophelia_spark.sparkContext

    def clear_cache(self):
        self.ophelia_spark.catalog.clearCache()
        OpheliaSpark.__logger.info("Clear Spark Cache Success")
