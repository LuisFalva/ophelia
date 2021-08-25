class Ophelia:

    def __init__(self, app_name=None, no_mask=False):
        from ._info import OpheliaInfo
        from .write.spark_write import Write
        from .read.spark_read import Read
        from .session.spark import OpheliaSpark
        Read()
        Write()
        m = OpheliaInfo()
        m.print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.SC = self.Spark.build_spark_context()
