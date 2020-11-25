from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (when, row_number, lit, count, first, sum as spark_sum,
                                   min as spark_min, max as spark_max, mean, stddev, variance)

__all__ = ["Rollings", "RollingsWrapper"]


class Rollings(object):

    @staticmethod
    def spark_methods():
        return {
            'sum': spark_sum,
            'min': spark_min,
            'max': spark_max,
            'mean': mean,
            'stddev': stddev,
            'var': variance,
            'first': first,
            'count': count,
        }

    @staticmethod
    def rolling_down(self, op_col, nat_order, min_p=2, window=2, method='sum'):
        w = Window.orderBy(nat_order).rowsBetween(
            Window.currentRow - (window - 1), Window.currentRow)
        if method == 'count':
            if isinstance(op_col, list):
                rolling = Rollings.spark_methods()[method]([c for c in op_col][0])
                return self.select(rolling)
            rolling = Rollings.spark_methods()[method](op_col).over(w).alias(f'{op_col}_rolling_{method}')
            return self.select('*', rolling)
        _unbounded_w = Window.orderBy(nat_order).rowsBetween(
            Window.unboundedPreceding, Window.currentRow)
        rolling = when(
            row_number().over(_unbounded_w) >= min_p,
            Rollings.spark_methods()[method](op_col).over(w),
            ).otherwise(lit(None)).alias(f'{op_col}_rolling_{method}')
        return self.select('*', rolling)


class RollingsWrapper(object):

    DataFrame.rollingDown = Rollings.rolling_down
