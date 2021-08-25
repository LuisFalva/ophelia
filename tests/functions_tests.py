import unittest
import os

from datetime import datetime
from pyspark.sql import SparkSession
from ophelia.functions import NullDebug, CorrMat


class TestFunctions(unittest.TestCase):
    __spark_session = SparkSession.builder.appName("unittest_functions").master("local").getOrCreate()
    __resources_path = str(os.getcwd()) + "/test_resources/"

    NullDebug().null_clean(df, 'date')