from com.ophelia.process.SparkInit import OpheliaSparkInit
from com.ophelia.process.DataTransform import MazterizeData
from com.ophelia.functions import Arrays, Parse, DataFrame, RDD


class Ophelia(object):
    
    def __init__(self, app_name=None):
        print("\n========================================================================")
        print("-Ophelia: ¡Hullo! My Name Is Ophelia, I Am Pleased To Meet You     [...]")
        print("-Ophelia: I Am An Artificial Assistant For Intelligent Investment  [...]")
        print("-Ophelia: Welcome To Your Asset Allocation System                  [...]\n")
        print("\n-Ophelia: V For VenData                                            [...]")
        print("========================================================================\n")
        print("                    - By. Vendetta Gentleman Club -                     \n")
        print("      █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █")
        print("      █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █")
        print("      █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █")
        print("      █ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ █ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ █ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █")
        print("      █ █ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ █ █")
        print("      █ █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █ █")
        print("      █ █ ╬ ╬ ▓ █ █ █ ╬ ╬ ╬ █ █ █ █ ╬ █ █ █ █ ╬ ╬ ╬ █ █ █ ▓ ╬ ╬ █ █")
        print("      █ █ █ ╬ ╬ ▓ ▓ █ █ █ █ █ █ █ ╬ ╬ ╬ █ █ █ █ █ █ █ ▓ ▓ ╬ ╬ █ █ █")
        print("      █ █ █ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ █ █ █")
        print("      █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █")
        print("      █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █")
        print("      █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █")
        print("      █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █")
        print("      █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █")
        print("      █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █")
        print("      █ █ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ █ █")
        print("      █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █")
        print("\n========================================================================")
        ophelia_spark = OpheliaSparkInit()
        spark = ophelia_spark.spark_session()
        sc = ophelia_spark.start_spark_context(spark)
        
        self.spark = spark
        self.sc = sc
        self.mazterize = MazterizeData()
        self.arrays = Arrays()
        self.parse = Parse()
        self.dataframe = DataFrame()
        self.rdd = RDD()
        
        self.greetings = "¡Hullo! I Am Ophelia The Very First AI Investment Assistant, Pleased To Meet You [...]"
        self.whoAreYou = "I Am British, From A Little Town Called Ely, East Of England. ¿Do You Want A Cup Of Tee? [...]"