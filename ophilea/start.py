from ..ophilea._info import OphileaInfo
from ..ophilea.write.spark_write import Write
from ..ophilea.read.spark_read import Read
from ..ophilea.session.spark import OpheliaSpark


class Ophilea:

    def __init__(self, app_name=None, no_mask=True):
        m = OphileaInfo()
        m.print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.Read = Read()
        self.Write = Write()
