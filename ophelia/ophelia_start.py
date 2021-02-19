from ophelia.ophelia.ophelia_init import OpheliaInit
from ophelia.ophelia.write.spark_write import Write
from ophelia.ophelia.session.spark import OpheliaSpark
from ophelia.ophelia.read.spark_read import Read


class Ophelia:

    def __init__(self, app_name=None, no_mask=True):
        m = OpheliaInit()
        m.print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.Read = Read()
        self.Write = Write()
