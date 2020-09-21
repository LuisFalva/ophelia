from com.ophelia import Read
from com.ophelia import Write
from com.ophelia import OpheliaMask
from com.ophelia import OpheliaSpark
from com.ophelia import Builder
from com.ophelia import ListUtils, ParseUtils, DataFrameUtils, RDDUtils
from enquire.engine.OpheliaClassifier import OpheliaClassifier


class OpheliaVendata:

    def __init__(self, app_name=None):

        mask = OpheliaMask()
        mask.print_info()
        
        self.opSpark = OpheliaSpark()
        self.opSession = self.opSpark.build_spark_session(app_name)
        self.opRead = Read()
        self.opWrite = Write()
        self.opArray = ListUtils()
        self.opParse = ParseUtils()
        self.opDf = DataFrameUtils()
        self.opRdd = RDDUtils()
        self.opBuild = Builder()
        self.opClassify = OpheliaClassifier()
