import numpy as np
from pyspark.ml.linalg import DenseVector
from pyspark.sql.types import Row
from pyspark.sql.catalog import Catalog
from ophelia.ophelib.utils.logger import OpheliaLogger

logger = OpheliaLogger()


class DataSUtils:

    def __init__(self, sql_ctx):
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx and sql_ctx._sc

    @staticmethod
    def spark_to_numpy(df):
        cache_df = df.cache()
        to_numpy = np.asarray(cache_df.rdd.map(lambda x: x[0]).collect())
        Catalog.clearCache()
        return to_numpy

    def numpy_to_vector_assembler(self, numpy_array, label_t):
        data_set = self._sc.parallelize(numpy_array)
        data_rdd = data_set.map(lambda x: (Row(features=DenseVector(x), label=label_t)))
        return data_rdd.toDF()
