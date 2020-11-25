from ophelia.ophelib import OpheliaMask
from ophelia.ophelib.utils.logger import OpheliaLogger
from ophelia.ophelib.read.spark_read import Read
from ophelia.ophelib.write.spark_write import Write
from ophelia.ophelib.session.spark import OpheliaSpark
from ophelia.ophelib.ml.ml_miner import MLMiner
from ophelia.ophelib.utils.list_utils import ListUtils
from ophelia.ophelib.utils.df_utils import DataFrameUtils
from ophelia.ophelib.utils.rdd_utils import RDDUtils
from ophelia.ophelib.utils.data_struct_utils import DataSUtils


class Ophelia:

    def __init__(self, app_name=None, no_mask=True):
        mask = OpheliaMask()
        mask.print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.Read = Read()
        self.Write = Write()
        self.Lists = ListUtils()
        self.DataS = DataSUtils(self.SparkSession)
        self.Df = DataFrameUtils()
        self.Rdd = RDDUtils()
        self.MLMiner = MLMiner()
        self.logger = OpheliaLogger()
