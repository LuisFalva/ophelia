from typing import AnyStr, Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, explode, struct, array, round as spark_round
from spark import SparkMethods

__all__ = ["TransposeDataFrame", "TransposeDataFrameWrapper"]


class TransposeDataFrame(object):

    @staticmethod
    def narrow_format(self: DataFrame, fix_cols: List, new_cols=None):
        """
        Narrow format method will reshape from wide tabular table to
        narrow multidimensional format table for increasing push-down
        predicate native Spark performance
        :param self: Spark DataFrame inheritance object
        :param fix_cols: str, column name as fix column
        :param new_cols: str or list, with pivot and value new column names
        :return: narrow Spark DataFrame
        """
        if isinstance(new_cols, str):
            pivot_col, value_col = new_cols.split(',')[0], new_cols.split(',')[1]
        elif new_cols is not None and isinstance(new_cols, list):
            pivot_col, value_col = new_cols[0], new_cols[1]
        else:
            pivot_col, value_col = 'no_name_pivot_col', 'no_name_value_col'
        cols, dtype = zip(*[(c, t) for (c, t) in self.dtypes if c not in fix_cols])

        if len(set(map(lambda x: x[-1], dtype[1:]))) != 1:
            raise AssertionError("Column Type Must Be The Same 'DataType'")

        generator_explode = explode(array([
            struct(lit(c).alias(pivot_col), col(c).alias(value_col)) for c in cols
        ])).alias('column_explode')
        column_to_explode = [f'column_explode.{pivot_col}', f'column_explode.{value_col}']

        return self.select(fix_cols + [generator_explode]).select(fix_cols + column_to_explode)

    @staticmethod
    def wide_format(self: DataFrame, group_by: AnyStr, pivot_col: AnyStr, agg_dict: Dict[AnyStr, AnyStr],
                    rnd: int = 4, rep: int = 20):
        """
        Wide format method will reshape from narrow multidimensional
        table to wide tabular format table for feature wide table
        :param self: Spark DataFrame inheritance object
        :param group_by: str, name for group DataFrame
        :param pivot_col: str, name for wide pivot column
        :param agg_dict: dict, with wide Spark function
        :param rnd: int, for round wide value
        :param rep: int, repartition threshold for wide partition optimization
        :return: wide Spark DataFrame
        """
        agg_list = []
        keys, values = list(agg_dict.keys())[0], list(agg_dict.values())[0]
        for k in agg_dict:
            for i in range(len(agg_dict[k].split(','))):
                strip_string = agg_dict[k].replace(' ', '').split(',')
                agg_list.append(spark_round(SparkMethods()[strip_string[i]](k), rnd).alias(f'{strip_string[i]}_{k}'))
        group_by_expr = col(group_by).alias(f'{group_by}_{pivot_col}')

        if len(list(agg_dict)) == 1:
            pivot_df = self.groupBy(group_by_expr).pivot(pivot_col).agg(*agg_list).repartition(rep).na.fill(0)
            renamed_cols = [col(c).alias(f"{c}_{keys}_{values}") for c in pivot_df.columns[1:]]
            return pivot_df.select(f'{group_by}_{pivot_col}', *renamed_cols)

        return self.groupBy(group_by_expr).pivot(pivot_col).agg(*agg_list).repartition(rep).na.fill(0)


class TransposeDataFrameWrapper(object):

    DataFrame.toWide = TransposeDataFrame.wide_format
    DataFrame.toNarrow = TransposeDataFrame.narrow_format
