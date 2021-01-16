import os
from dask import dataframe as dask_df, array as dask_arr
from pyspark.sql import DataFrame

__all__ = ["DaskSpark", "DaskSparkWrapper"]


class DaskSpark(object):

    @staticmethod
    def __spark_to_dask(self):
        tmp_path = os.getcwd() + '/data/dask_tmp/'
        self.coalesce(1).write.mode('overwrite').parquet(tmp_path)
        return dask_df.read_parquet(tmp_path)

    @staticmethod
    def spark_to_series(self, column_series):
        dask_df = DaskSpark.__spark_to_dask(self)
        series = dask_df[column_series]
        list_dask = series.to_delayed()
        full = [dask_arr.from_delayed(i, i.compute().shape, i.compute().dtype) for i in list_dask]
        return dask_arr.concatenate(full)

    @staticmethod
    def spark_to_numpy(self, column_series=None):
        if column_series is not None:
            dask_array = self.toPandasSeries(column_series)
            return dask_arr.from_array(dask_array.compute())
        else:
            dask_pandas_series = DaskSpark.__spark_to_dask(self)
            return dask_pandas_series.to_dask_array()


class DaskSparkWrapper(object):

    DataFrame.toPandasSeries = DaskSpark.spark_to_series
    DataFrame.toNumpyArray = DaskSpark.spark_to_numpy
