class Ophilea:

    def __init__(self, app_name=None, no_mask=True):
        from ..ophelia._info import OphileaInfo
        from ..ophelia.write.spark_write import Write
        from ..ophelia.read.spark_read import Read
        from ..ophelia.session.spark import OpheliaSpark
        Read()
        Write()
        m = OphileaInfo()
        m.print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.SC = self.Spark.build_spark_context()
