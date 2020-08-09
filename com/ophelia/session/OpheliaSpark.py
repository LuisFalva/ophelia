from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from com.ophelia.utils.logger import OpheliaLogger


class OpheliaSpark:

    __logger = OpheliaLogger()

    @staticmethod
    def __build_spark_app(app_name):
        return SparkSession.builder.appName(app_name).getOrCreate()

    @staticmethod
    def __build_spark():
        return SparkSession.builder.appName("No 'appName' configured").getOrCreate()

    @staticmethod
    def build_spark_session(app_name: str = None) -> SparkSession:
        """
        Build spark session is the builder method to initialize spark session under the hood of Ophelia
        :param app_name: str, app name description for spark job name
        :return: spark session
        """
        if not app_name:
            OpheliaSpark.__logger.info("Build OpheliaSpark Session")
            ophelia_spark = OpheliaSpark.__build_spark()
            OpheliaSpark.__logger.info("OpheliaSpark Session Version: " + ophelia_spark.version)
            OpheliaSpark.__logger.warning("Please, Be Aware To Set App Name Next Time...")
            return ophelia_spark
        else:
            OpheliaSpark.__logger.info("Initializing OpheliaSpark Session")
            ophelia_spark = OpheliaSpark.__build_spark_app(app_name=app_name)
            OpheliaSpark.__logger.info("OpheliaSpark Session Version: " + ophelia_spark.version)
            OpheliaSpark.__logger.info("This Is: '" + ophelia_spark.sparkContext.appName + "' Project")
            return ophelia_spark

    @staticmethod
    def build_spark_context() -> SparkContext:
        """
        Build spark context is the builder method to initialize spark context given a spark session
        :return: spark context
        """
        OpheliaSpark.__logger.info("OpheliaSpark Context Initialized Successfully")
        return OpheliaSpark.build_spark_session().sparkContext

    @staticmethod
    def clear_cache():
        spark = OpheliaSpark.__build_spark()
        spark.catalog.clearCache()
        OpheliaSpark.__logger.info("Clear Spark Cache")
