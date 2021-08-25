import unittest
import os

from datetime import datetime
from pyspark.sql import SparkSession
from ophelia.functions import NullDebug, CorrMat


class TestFunctions(unittest.TestCase):
    __spark_session = SparkSession.builder.appName("unittest_functions").master("local").getOrCreate()
    __resources_path = str(os.getcwd()) + "/test_resources/"

    NullDebug().null_clean(df, 'date')

    @staticmethod
    def get_tables_dict(conf_path):
        conf = Conf(TestConfTableLoader.__resources_path + conf_path)
        tables_dict = ConfigReader.get_tables(TestConfTableLoader.__spark_session, conf,
                                              'config.stage_name', 'config.target_date',
                                              'config.input', 'tests.loading')
        return tables_dict

    def test_get_tables_daily(self):
        """
        Tests get_tables method
        """
        tables_dict = TestConfTableLoader.get_tables_dict('test_loading_daily.conf')
        self.assertEqual(type(tables_dict), dict)
        self.assertEqual(datetime.strptime('2017-12-10', '%Y-%m-%d').date(), tables_dict['dates']['today'])
        self.assertEqual(tables_dict['tla635'][1], datetime.strptime('2017-12-06', '%Y-%m-%d').date())