from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, sum, lit, round as spark_round
from ophelia.ophelib.utils import regex_expr

__all__ = ["DynamicPivot", "DynamicPivotWrapper"]


class DynamicPivot(object):

    @staticmethod
    def __expression(cols_list, expr):
        expr_dict = {
            'sum': '+'.join(cols_list),
            'sub': '-'.join(cols_list),
            'mul': '*'.join(cols_list),
            'div': '/'.join(cols_list),
        }
        return expr_dict[expr]

    @staticmethod
    def foreach_col(self, group_by, pivot_col, agg_dict, oper):
        func = []
        regex_keys = list(agg_dict.keys())
        regex_values = list(agg_dict.values())
        df = self.toMatrix(group_by, pivot_col, agg_dict)
        for i in range(len(regex_keys)):
            cols_list = df.selectRegex(regex_expr(regex_keys[i])).columns
            expression = expr(DynamicPivot.__expression(cols_list, oper))
            func.append(expression.alias(f'{regex_keys[i]}_{regex_values[i]}_{oper}'))
        return df.select('*', *func)

    @staticmethod
    def resume_dataframe(self, group_by=None, new_col=None):
        cols_types = [k for k, v in self.dtypes if v != 'string']
        if group_by is None:
            try:
                agg_df = self.agg(*[sum(c).alias(c) for c in cols_types])
                return agg_df.withColumn(new_col, lit('+++ total')).select(new_col, *cols_types)
            except Py4JJavaError as e:
                raise AssertionError(f"empty expression found. {e}")
        return self.groupBy(group_by).agg(*[sum(c).alias(c) for c in cols_types])

    @staticmethod
    def tab_table(self, group_by, pivot_col, agg_dict, oper='sum'):
        sum_by_col_df = DynamicPivot.foreach_col(self, group_by, pivot_col, agg_dict, oper)
        return sum_by_col_df.union(DynamicPivot.resume_dataframe(sum_by_col_df, new_col=self.columns[0]))

    @staticmethod
    def cross_pct(self, group_by, pivot_col, agg_dict, operand='sum', cols=None):
        sum_by_col_df = DynamicPivot.foreach_col(self, group_by, pivot_col, agg_dict, operand)
        union_df = sum_by_col_df.union(DynamicPivot.resume_dataframe(sum_by_col_df, new_col=self.columns[0]))
        key_list = list(agg_dict.keys())
        func = []
        for i in range(len(key_list)):
            tmp_df = sum_by_col_df.selectRegex(regex_expr([key_list[i]]))
            fix_df = tmp_df.selectRegex(regex_expr([operand]))
            pivot_list = tmp_df.drop(fix_df.columns[0]).columns
            for ix in range(len(pivot_list)):
                operate_cols = [pivot_list[ix], fix_df.columns[0]]
                dynamic_expr = DynamicPivot.__expression(operate_cols, 'div')
                func.append(spark_round(expr(dynamic_expr), 4).alias(f'{pivot_list[ix]}_prop'))
        if cols == 'all':
            return union_df.select('*', *func)
        else:
            return union_df.select(f'{group_by}_{pivot_col}', *func)


class DynamicPivotWrapper(object):

    DataFrame.foreachCol = DynamicPivot.foreach_col
    DataFrame.resumeDF = DynamicPivot.resume_dataframe
    DataFrame.tabularTable = DynamicPivot.tab_table
    DataFrame.crossPct = DynamicPivot.cross_pct
