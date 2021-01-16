#from spark import OpheliaMask
from spark.write.spark_write import Write
from spark.session.spark import OpheliaSpark
from spark.ml import OpheliaML
from spark.utils import OpheliaUtils
from spark.functions import OpheliaWrappers
from spark.read import OpheliaReadWrapper


class Ophelia:

    def __init__(self, app_name=None, no_mask=True):
        #mask = OpheliaMask()
        #mask.print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.Read = OpheliaReadWrapper()
        self.Write = Write()
        self.Utils = OpheliaUtils()
        self.ML = OpheliaML()
        self.Wrapper = OpheliaWrappers()
