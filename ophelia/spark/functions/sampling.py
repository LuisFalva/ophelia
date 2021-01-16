import random
from pyspark.sql import types as t
from pyspark.sql import DataFrame, functions as f

__all__ = ["DynamicSampling", "DynamicSamplingWrapper"]


class DynamicSampling(object):

    @staticmethod
    def empty_scan(self):
        cols = self.columns
        schema = t.StructType([t.StructField(col_name, t.StringType(), True) for col_name in cols])
        return self.sql_ctx.createDataFrame(self.sql_ctx._sc.emptyRDD(), schema)

    @staticmethod
    def __id_row_number(df, alias):
        return df.select('*', f.monotonically_increasing_id().alias(alias))

    @staticmethod
    def union_all(dfs):
        first = dfs[0]
        union_dfs = first.sql_ctx._sc.union([df.rdd for df in dfs])
        return first.sql_ctx.createDataFrame(union_dfs, first.schema)

    @staticmethod
    def sample_n(self, n):
        _ = DynamicSampling.__id_row_number(self, 'n')
        max_n = _.select('n').orderBy(f.col('n').desc()).limit(1).cache()
        sample_list = []
        for sample in range(n):
            sample_list.append(_.where(f.col('n') == random.randint(0, max_n.collect()[0][0])))
        return DynamicSampling.union_all(sample_list).drop('n')


class DynamicSamplingWrapper(object):

    DataFrame.emptyScan = DynamicSampling.empty_scan
    DataFrame.sampleN = DynamicSampling.sample_n
