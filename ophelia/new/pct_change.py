from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import col, lag


class PctChangeDF:

    @staticmethod
    def __build_pct_change_function(x, t, w):
        if isinstance(x, str):
            return ((col(x) / lag(col(x), offset=t).over(w)) - 1).alias(x)
        elif isinstance(x, list):
            return [(col(c) / lag(col(c), offset=t).over(w) - 1).alias(c) for c in x]

    @staticmethod
    def __single_order_by_pct(self, periods, order_by, pct_col):
        win = Window.orderBy(col(order_by).desc())
        function = PctChangeDF.__build_pct_change_function(x=pct_col, t=periods, w=win)
        return self.select(function)

    @staticmethod
    def __single_partition_pct(self, periods, partition_by, order_by, pct_col):
        win = Window.partitionBy(partition_by).orderBy(col(order_by).desc())
        function = PctChangeDF.__build_pct_change_function(x=pct_col, t=periods, w=win)
        return self.select(function)

    @staticmethod
    def __multi_order_by_pct(self, periods, order_by, pct_cols):
        win = Window.orderBy(col(order_by).desc())
        function = PctChangeDF.__build_pct_change_function(x=pct_cols, t=periods, w=win)
        return self.select(*function)

    @staticmethod
    def __multi_partition_pct(self, periods, partition_by, order_by, pct_cols):
        win = Window.partitionBy(partition_by).orderBy(col(order_by).desc())
        function = PctChangeDF.__build_pct_change_function(x=pct_cols, t=periods, w=win)
        return self.select(*function)

    @staticmethod
    def add_pct_change():
        def pct_change(self, periods=1, partition_by=None, order_by=None, pct_cols=None):
            if partition_by is None:
                if isinstance(pct_cols, str):
                    return PctChangeDF.__single_order_by_pct(self, periods, order_by, pct_cols)
                elif isinstance(pct_cols, list):
                    return PctChangeDF.__multi_order_by_pct(self, periods, order_by, pct_cols)
            elif partition_by is not None:
                if isinstance(pct_cols, str):
                    return PctChangeDF.__single_partition_pct(self, periods, partition_by, order_by, pct_cols)
                elif isinstance(pct_cols, list):
                    return PctChangeDF.__multi_partition_pct(self, periods, partition_by, order_by, pct_cols)
        DataFrame.pct_change = pct_change

    @staticmethod
    def add_remove_element():
        def remove_element(self, column_to_remove):
            primary_list = self.columns
            [primary_list.remove(c) for c in column_to_remove]
            return primary_list
        DataFrame.remove_element = remove_element
