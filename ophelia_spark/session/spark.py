from pyspark.sql import SparkSession

from .._logger import OpheliaLogger


class OpheliaSpark:

    __logger = OpheliaLogger()

    def __init__(self):
        self.ophelia_spark = None

    def __build_spark_app(self, app_name):
        self.ophelia_spark = SparkSession.builder.appName(app_name).getOrCreate()

    def __build_spark(self):
        self.ophelia_spark = SparkSession.builder.appName(
            "No 'appName' configured"
        ).getOrCreate()

    def __app_name(self):
        return self.ophelia_spark.sparkContext.appName

    def __spark_version(self):
        return self.ophelia_spark.version

    def __spark_ui_port(self):
        """
        Print spark UI port server for tracking Spark activity
        :return: spark ui port address
        """
        return self.ophelia_spark._sc.uiWebUrl

    def build_spark_session(self, app_name: str = None) -> SparkSession:
        """
        Build spark session is the builder method to initialize spark session under the hood of Ophelia
        :param app_name: str, app name description for spark job name
        :return: spark session
        """
        if not app_name:
            OpheliaSpark.__logger.info("Build Spark Session")
            self.__build_spark()
            OpheliaSpark.__logger.info("Spark Version: " + self.__spark_version())
            OpheliaSpark.__logger.warning(
                "Please, Be Aware To Set App Name Next Time..."
            )
            OpheliaSpark.__logger.info("Spark UI Address: '" + self.__spark_ui_port())
            return self.ophelia_spark
        else:
            OpheliaSpark.__logger.warning("Initializing Spark Session")
            self.__build_spark_app(app_name=app_name)
            OpheliaSpark.__logger.info("Spark Version: " + self.__spark_version())
            OpheliaSpark.__logger.info("This Is: '" + self.__app_name() + "' App")
            OpheliaSpark.__logger.info("Spark UI Address: " + self.__spark_ui_port())
            return self.ophelia_spark

    def build_spark_context(self):
        """
        Build spark context is the builder method to initialize spark context given a spark session
        :return: spark context
        """
        OpheliaSpark.__logger.info("Spark Context Initialized Successfully")
        return self.ophelia_spark.sparkContext

    def clear_cache(self):
        self.ophelia_spark.catalog.clearCache()
        OpheliaSpark.__logger.info("Clear Spark Cache Successfully")

    @staticmethod
    def ophelia_active_session():
        OpheliaSpark.__logger.info("Ophelia Active Session")
        return SparkSession.getActiveSession()
