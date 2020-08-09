from com.ophelia import OpheliaMask
from com.ophelia import ListUtils, ParseUtils, DataFrameUtils, RDDUtils
from com.ophelia import OpheliaSpark
from com.ophelia import Read
from com.ophelia import Write
from enquire.engine.OpheliaClassifier import OpheliaClassifier


class OpheliaVendata:

    def __init__(self, app_name=None):

        mask = OpheliaMask()
        mask.print_info()
        
        self.ophelia_spark = OpheliaSpark()
        self.ophelia_session = self.ophelia_spark.build_spark_session(app_name)
        self.ophelia_read = Read()
        self.ophelia_write = Write()
        self.ophelia_array = ListUtils()
        self.ophelia_parse = ParseUtils()
        self.ophelia_df = DataFrameUtils()
        self.ophelia_rdd = RDDUtils()
        self.ophelia_class = OpheliaClassifier()
