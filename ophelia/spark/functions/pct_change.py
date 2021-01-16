from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import col, lag
from ophelia.spark.utils import regex_expr, feature_pick

__all__ = ["PctChangeDF", "PctChangeWrapper"]


class PctChangeDF(object):

    @staticmethod
    def __build_pct_change_function(x, t, w):
        if isinstance(x, str):
            list_x = [x]
            return list(map(lambda c: (col(c) / lag(col(c), offset=t).over(w) - 1).alias(c), list_x))
        elif isinstance(x, list):
            return list(map(lambda c: (col(c) / lag(col(c), offset=t).over(w) - 1).alias(c), x))

    @staticmethod
    def __build_window_partition(self, periods, order_by, pct_cols, partition_by=None, desc=False):
        if desc:
            if partition_by is None:
                win = Window.orderBy(col(order_by).desc())
            else:
                win = Window.partitionBy(partition_by).orderBy(col(order_by).desc())
        else:
            if partition_by is None:
                win = Window.orderBy(col(order_by))
            else:
                win = Window.partitionBy(partition_by).orderBy(col(order_by))
        function = PctChangeDF.__build_pct_change_function(x=pct_cols, t=periods, w=win)
        return self.select(function)

    @staticmethod
    def __infer_sort_column(self):
        regex_match = ['year', 'Year', 'date', 'Date', 'month', 'Month', 'day', 'Day']
        regex_list = self.selectRegex(regex_expr(regex_match)).columns
        if len(regex_list) > 0:
            return regex_list
        elif len(feature_pick(self)['date']) > 0:
            return feature_pick(self)['date']
        else:
            return feature_pick(self)['string']

    @staticmethod
    def __infer_lag_column(self):
        if len(feature_pick(self)['double']) > 0:
            return feature_pick(self)['double']
        elif len(feature_pick(self)['float']) > 0:
            return feature_pick(self)['float']
        elif len(feature_pick(self)['long']) > 0:
            return feature_pick(self)['long']
        else:
            return feature_pick(self)['int']

    @staticmethod
    def pct_change(self, periods=1, order_by=None, pct_cols=None, partition_by=None, desc=None):
        if (order_by is None) and (pct_cols is None):
            infer_sort = PctChangeDF.__infer_sort_column(self)[0]
            infer_lag = PctChangeDF.__infer_lag_column(self)[1]
            return PctChangeDF.__build_window_partition(self, periods, infer_sort, infer_lag)
        return PctChangeDF.__build_window_partition(self, periods, order_by, pct_cols, partition_by, desc)

    @staticmethod
    def remove_element(self, col_remove):
        primary_list = self.columns
        [primary_list.remove(c) for c in col_remove]
        return primary_list


class PctChangeWrapper(object):

    DataFrame.pctChange = PctChangeDF.pct_change
    DataFrame.remove_element = PctChangeDF.remove_element
