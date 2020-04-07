import sys
from pyspark.sql import SparkSession

class OpheliaSparkInit(object):
    """
    Class object for input parameters.
    :param NAME_PARAM: "DESCRIPTION HERE"
    """
    @staticmethod
    def spark_session(app_name=None):
        """
        Class object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"
        """
        if app_name is None:
            print("-Ophelia[INFO]: Initializing Spark Session [...]")
            spark = SparkSession.builder.appName("set_app_name_next_time").getOrCreate()
            print("-Ophelia[INFO]: Spark Session Initialized, Version:", str(spark.version), "[...]")
            print("-Ophelia[WARN]: Please, Be Aware To Set App Name Next Time [...]")
            print("===="*18)
            return spark
        
        print("-Ophelia[INFO]: Initializing Spark Session [...]")
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("-Ophelia[INFO]: Spark Context Initialized Successfully [...]")
        print("===="*18)        
        return spark

    @staticmethod
    def start_spark_context(spark_session=None):
        """
        Class object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"
        """
        print("-Ophelia[INFO]: Initializing Spark Context [...]")
        if spark_session is None:
            print("-Ophelia[FAIL]: Please, set spark session argument [...]")
            return None
        
        print("-Ophelia[INFO]: Spark Context Initialized Successfully [...]")
        print("===="*18)
        return spark_session.sparkContext