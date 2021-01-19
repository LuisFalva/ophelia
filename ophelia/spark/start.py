from ophelia.spark.mask import OpheliaMask
from ophelia.spark.write.spark_write import Write
from ophelia.spark.session.spark import OpheliaSpark
from ophelia.spark.ml import OpheliaML
from ophelia.spark.read.spark_read import Read


class Ophelia:

    def __init__(self, app_name=None, no_mask=True):
        m = OpheliaMask()
        m.print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.Read = Read()
        self.Write = Write()
        self.ML = OpheliaML()
