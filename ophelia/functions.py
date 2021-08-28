import os
import re
import random
import pandas as pd
from itertools import chain
from py4j.protocol import Py4JJavaError
from dask import dataframe as dask_df, array as dask_arr
from pyspark.sql import DataFrame, Window
from pyspark.sql.column import _to_seq
from pyspark.sql.functions import (
    when, col, lit, row_number, monotonically_increasing_id, create_map, explode, struct, array, round as spark_round,
    lag, expr, sum, broadcast, count, isnan
)
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from . import SparkMethods, OpheliaFunctionsException
from .generic import remove_duplicate_element, feature_pick, regex_expr

__all__ = ["NullDebugWrapper", "CorrMatWrapper", "ShapeWrapper", "MapItemsWrapper",
           "RollingWrapper", "DynamicSamplingWrapper", "SelectWrapper",
           "ReshapeWrapper", "PctChangeWrapper", "CrossTabularWrapper"]


class NullDebug:

    @staticmethod
    def __cleansing_list(self, partition_by=None, offset: float = 0.5):
        if partition_by is None:
            raise TypeError(f"'partition_by' required parameter, invalid {partition_by} input.")
        return self.toNarrow(partition_by, ['pivot', 'value']).groupBy('pivot') \
            .agg(count(when(isnan('value') | col('value').isNull(), 'value')).alias('null_count')) \
            .select('*', (col('null_count') / self.Shape[0]).alias('null_pct')) \
            .where(col('null_pct') <= offset).uniqueRow('pivot')

    @staticmethod
    def null_clean(self, partition_by, offset):
        """
        Null clean method will be on charge to clean and debug all kind of null types in
        your Spark DataFrame within a determined offset of proportionality
        :param self: DataFrame object heritage
        :param partition_by: str, name of partition column
        :param offset: float, number for offset controller
        :return: new Spark DataFrame without columns that had more Nulls than the set offset
        """
        if partition_by:
            cleansing_list = NullDebug.__cleansing_list(self, partition_by, offset)
            return self.select(partition_by, *cleansing_list)
        gen_part = self.select(monotonically_increasing_id().alias('partition_id'), "*")
        cleansing_list = NullDebug.__cleansing_list(gen_part, 'partition_id', offset)
        return gen_part.select('partition_id', *cleansing_list)


class NullDebugWrapper(DataFrame):
    """
    Class NullDebugWrapper is a class for wrapping methods from NullDebug class
    adding this functionality to Spark DataFrame class
    """
    DataFrame.nullDebug = NullDebug.null_clean


class CorrMat:

    @staticmethod
    def __corr(pair):
        """
        Corr private method distribute pandas series operations for correlation between RDD
        :param pair: RDD pair of elements
        :return: product of correlation matrix
        """
        (prod1, series1), (prod2, series2) = pair
        corr = pd.Series(series1).corr(pd.Series(series2))
        return prod1, prod2, float(corr)

    @staticmethod
    def __build_corr_label(mtd):
        min_level = 0.1
        mid_level = 0.3
        max_level = 0.5
        return when(col(f'{mtd}_coeff') < min_level, lit('very_low')).otherwise(
            when((col(f'{mtd}_coeff') < min_level) & (col(f'{mtd}_coeff') <= mid_level), lit('low')).otherwise(
                when((col(f'{mtd}_coeff') < mid_level) & (col(f'{mtd}_coeff') <= max_level), lit('mid')).otherwise(
                    when(col(f'{mtd}_coeff') > max_level, lit('high')))))

    @staticmethod
    def cartesian_rdd(self, default_pivot, rep=20):
        if default_pivot is None:
            raise ValueError("'default_pivot' must be specified")
        rep_df = self.repartition(rep)
        numerical_cols = rep_df.columns[1:]
        to_wide_rdd = rep_df.rdd.map(lambda x: (x[default_pivot], [x[c] for c in numerical_cols]))
        return to_wide_rdd.cartesian(to_wide_rdd)

    @staticmethod
    def correlation_matrix(self, pivot_col=None, method='pearson', offset=0.7, rep=20):
        default_pivot = pivot_col if pivot_col is not None else self.columns[0]
        cartesian_rdd = CorrMat.cartesian_rdd(self, default_pivot=default_pivot, rep=rep)
        new_col_list = [f'{default_pivot}_m_dim', f'{default_pivot}_n_dim', f'{method}_coeff']
        corr_df = cartesian_rdd.map(CorrMat.__corr).toDF(schema=new_col_list)
        offset_condition = when(col(f'{method}_coeff') >= offset, lit(1.0)).otherwise(0.0)
        corr_label = CorrMat.__build_corr_label(method)
        return corr_df.select('*', offset_condition.alias('offset'), corr_label.alias(f'{method}_label'))

    @staticmethod
    def unique_row(self, col):
        categories_rows = self.select(col).groupBy(col).count().collect()
        return sorted([categories_rows[i][0] for i in range(len(categories_rows))])

    @staticmethod
    def vector_assembler(self, cols_name):
        vec_assembler = VectorAssembler(inputCols=cols_name, outputCol='features')
        return vec_assembler.transform(self)

    @staticmethod
    def spark_correlation(self, group_by, pivot_col, agg_dict, method='pearson'):
        categories_list = self.uniqueRow(pivot_col)
        matrix_df = self.toWide(group_by=group_by, pivot_col=pivot_col, agg_dict=agg_dict)
        vec_df = matrix_df.vecAssembler(matrix_df.columns[1:])
        corr_test = Correlation.corr(vec_df, 'features', method) \
            .select(col(f'{method}(features)').alias(f'{method}_features'))
        corr_cols = [f"{method}_{c}_{b}" for c in categories_list for b in categories_list]
        extract = (lambda row: tuple(float(x) for x in row[f'{method}_features'].values))
        return corr_test.rdd.map(extract).toDF(corr_cols)


class CorrMatWrapper(DataFrame):

    DataFrame.uniqueRow = CorrMat.unique_row
    DataFrame.cartRDD = CorrMat.cartesian_rdd
    DataFrame.corrMatrix = CorrMat.correlation_matrix
    DataFrame.vecAssembler = CorrMat.vector_assembler
    DataFrame.corrStat = CorrMat.spark_correlation


class Shape:

    @staticmethod
    def shape(self):
        if len(self.columns) == 1:
            return self.count(),
        return self.count(), len(self.columns)


class ShapeWrapper(DataFrame):

    DataFrame.Shape = property(lambda self: Shape.shape(self))


class Rolling:

    @staticmethod
    def rolling_down(self, op_col, nat_order, min_p=2, window=2, method='sum'):
        w = Window.orderBy(nat_order).rowsBetween(
            Window.currentRow - (window - 1), Window.currentRow)
        if method == 'count':
            if isinstance(op_col, list):
                rolling = SparkMethods()[method]([c for c in op_col][0])
                return self.select(rolling)
            rolling = SparkMethods()[method](op_col).over(w).alias(f'{op_col}_rolling_{method}')
            return self.select('*', rolling)
        _unbounded_w = Window.orderBy(nat_order).rowsBetween(
            Window.unboundedPreceding, Window.currentRow)
        rolling = when(
            row_number().over(_unbounded_w) >= min_p,
            SparkMethods()[method](op_col).over(w),
            ).otherwise(lit(None)).alias(f'{op_col}_rolling_{method}')
        return self.select('*', rolling)


class RollingWrapper(DataFrame):

    DataFrame.rollingDown = Rolling.rolling_down


class DynamicSampling:

    @staticmethod
    def empty_scan(self):
        cols = self.columns
        schema = StructType([StructField(col_name, StringType(), True) for col_name in cols])
        return self.sql_ctx.createDataFrame(self.sql_ctx._sc.emptyRDD(), schema)

    @staticmethod
    def __id_row_number(df, alias):
        return df.select('*', monotonically_increasing_id().alias(alias))

    @staticmethod
    def union_all(dfs):
        first = dfs[0]
        union_dfs = first.sql_ctx._sc.union([df.rdd for df in dfs])
        return first.sql_ctx.createDataFrame(union_dfs, first.schema)

    @staticmethod
    def sample_n(self, n):
        _ = DynamicSampling.__id_row_number(self, 'n')
        max_n = _.select('n').orderBy(col('n').desc()).limit(1).cache()
        sample_list = []
        for sample in range(n):
            sample_list.append(_.where(col('n') == random.randint(0, max_n.collect()[0][0])))
        return DynamicSampling.union_all(sample_list).drop('n')


class DynamicSamplingWrapper(DataFrame):

    DataFrame.emptyScan = DynamicSampling.empty_scan
    DataFrame.simple_sample = DynamicSampling.sample_n


class Selects(DataFrame):

    def __init__(self, jdf, sql_ctx):
        super(Selects, self).__init__(jdf, sql_ctx)
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx and sql_ctx._sc

    @staticmethod
    def regex_expr(regex_name):
        if isinstance(regex_name, list):
            return [f'.*{regex}' for regex in regex_name]
        return [f'.*{regex_name}']

    @staticmethod
    def select_regex(self, reg_expr):
        # Todo: es posible llamar el atributo _jdf sin necesidad de quitar el decorador @staticmethod, se remueven por
        # Todo: que producen error de ejecucion por el momento
        # Todo: se deja el codigo muestra de version anterior
        """
        stream = self._jdf.columns
        regex_list = [line for regex in regex_expr for line in stream if re.compile(regex).match(line)]
        clean_regex_list = remove_duplicated_elements(regex_list)
        return DataFrame(self._jdf.select(clean_regex_list), self.sql_ctx)
        """
        stream = self.columns
        regex_list = [line for regex in reg_expr for line in stream if re.compile(regex).match(line)]
        clean_regex_list = remove_duplicate_element(regex_list)
        return self.select(clean_regex_list)

    @staticmethod
    def select_startswith(self, regex):
        cols_list = self.columns
        if isinstance(regex, list):
            return self.select([c for c in cols_list for reg in regex if c.startswith(reg)])
        return self.select([c for c in cols_list if c.startswith(regex)])

    @staticmethod
    def select_endswith(self, regex):
        cols_list = self.columns
        if isinstance(regex, list):
            return self.select([c for c in cols_list for reg in regex if c.endswith(reg)])
        return self.select([c for c in cols_list if c.endswith(regex)])

    @staticmethod
    def select_contains(self, regex):
        if isinstance(regex, list):
            contain_cols = [c for c in self.columns for reg in regex if reg in c]
            clean_regex_list = remove_duplicate_element(contain_cols)
            return self.select(clean_regex_list)
        contain_cols = [c for c in self.columns if regex in c]
        clean_regex_list = remove_duplicate_element(contain_cols)
        return self.select(clean_regex_list)

    @staticmethod
    def sort_columns_asc(self):
        # Todo: es posible llamar el atributo _jdf sin necesidad de quitar el decorador @staticmethod, se remueven por
        # Todo: que producen error de ejecucion por el momento
        # Todo: se deja el codigo muestra de version anterior
        """
        DataFrame(self._jdf.select(self._jdf.schema.fields), self.sql_ctx)

        cols = self._jdf.column
        print("java jdf object", cols)
        print("java jdf type", cols.__class__.__name__)
        return DataFrame(self._jdf.select(self._sc, cols), self.sql_ctx)
        """
        return self.select(*sorted(self.columns))

    @staticmethod
    def select_freqitems(self, cols, support=None):
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise ValueError("cols must be a list or tuple of column names as strings.")
        if not support:
            support = 0.01
        return DataFrame(self._jdf.stat().freqItems(_to_seq(self._sc, cols), support), self.sql_ctx)

    @staticmethod
    def __type_list(datatype, select_type):
        return [k for k, v in datatype if v in select_type]

    @staticmethod
    def select_strings(self):
        dtype = self.dtypes
        stype = ['string', 'str', 'AnyStr', 'char']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_integers(self):
        dtype = self.dtypes
        stype = ['int', 'integer']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_floats(self):
        dtype = self.dtypes
        stype = ['float']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_doubles(self):
        dtype = self.dtypes
        stype = ['double']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_decimals(self):
        dtype = self.dtypes
        stype = ['decimal']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_longs(self):
        dtype = self.dtypes
        stype = ['long', 'bigint']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_dates(self):
        dtype = self.dtypes
        stype = ['date', 'timestamp']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    @staticmethod
    def select_complex(self):
        dtype = self.dtypes
        stype = ['complex']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    @staticmethod
    def select_structs(self):
        dtype = self.dtypes
        stype = ['list', 'tuple', 'array', 'vector']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    @staticmethod
    def select_categorical(self):
        dtype = self.dtypes
        stype = ['string', 'long', 'bigint']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    @staticmethod
    def select_numerical(self):
        dtype = self.dtypes
        stype = ['double', 'decimal', 'integer', 'int', 'float', 'complex']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    def select_features(self, df):
        return {'string': self.select_strings(df),
                'int': self.select_integers(df),
                'long': self.select_longs(df),
                'double': self.select_doubles(df),
                'float': self.select_floats(df),
                'date': self.select_dates(df),
                'complex': self.select_complex(df),
                'struct': self.select_structs(df),
                'categorical': self.select_categorical(df),
                'numeric': self.select_numerical(df)}


class SelectWrapper(DataFrame):

    DataFrame.selectRegex = Selects.select_regex
    DataFrame.selectStartswith = Selects.select_startswith
    DataFrame.selectEndswith = Selects.select_endswith
    DataFrame.selectContains = Selects.select_contains
    DataFrame.sortColAsc = Selects.sort_columns_asc
    DataFrame.selectFreqItems = Selects.select_freqitems
    DataFrame.selectStrings = Selects.select_strings
    DataFrame.selectInts = Selects.select_integers
    DataFrame.selectFloats = Selects.select_floats
    DataFrame.selectDoubles = Selects.select_doubles
    DataFrame.selectDecimals = Selects.select_decimals
    DataFrame.selectLongs = Selects.select_longs
    DataFrame.selectDates = Selects.select_dates
    DataFrame.selectFeatures = Selects.select_features


class MapItem:

    @staticmethod
    def map_item(self, origin_col, map_col, map_val):
        map_expr = create_map([lit(x) for x in chain(*map_val.items())])
        return self.select('*', (map_expr[self[origin_col]]).alias(map_col))


class MapItemsWrapper(DataFrame):

    DataFrame.mapItem = MapItem.map_item


class Reshape(DataFrame):

    def __init__(self, df):
        super(Reshape, self).__init__(df._jdf, df.sql_ctx)
        self._df = df

    @staticmethod
    def narrow_format(self, fix_cols, new_cols=None):
        """
        Narrow format method will reshape from wide tabular table to
        narrow multidimensional format table for increasing push-down
        predicate native Spark performance
        :param self: Spark DataFrame inheritance object
        :param fix_cols: str, column name as fix column
        :param new_cols: str or list, with pivot and value new column names
        :return: narrow Spark DataFrame
        """
        try:
            if isinstance(new_cols, str):
                pivot_col, value_col = new_cols.split(',')[0], new_cols.split(',')[1]
            elif new_cols is not None and isinstance(new_cols, list):
                pivot_col, value_col = new_cols[0], new_cols[1]
            else:
                pivot_col, value_col = 'no_name_pivot_col', 'no_name_value_col'
            cols, dtype = zip(*[(c, t) for (c, t) in self.dtypes if c not in [fix_cols]])

            generator_explode = explode(array([
                struct(lit(c).alias(pivot_col), col(c).alias(value_col)) for c in cols
            ])).alias('column_explode')
            column_to_explode = [f'column_explode.{pivot_col}', f'column_explode.{value_col}']

            return Reshape(self.select([fix_cols] + [generator_explode]).select([fix_cols] + column_to_explode))
        except Exception as e:
            raise OpheliaFunctionsException(f"An error occurred while calling narrow_format() method: {e}")

    @staticmethod
    def wide_format(self, group_by, pivot_col, agg_dict, rnd=6, rep=20, rename_col=False):
        """
        Wide format method will reshape from narrow multidimensional
        table to wide tabular format table for feature wide table
        :param self: Spark DataFrame inheritance object
        :param group_by: str, name for group DataFrame
        :param pivot_col: str, name for wide pivot column
        :param agg_dict: dict, with wide Spark function
        :param rnd: int, for round wide value
        :param rep: int, repartition threshold for wide partition optimization
        :param rename_col: bool, boolean response for rename existing columns
        :return: wide Spark DataFrame
        """
        try:
            agg_list = []
            keys, values = list(agg_dict.keys())[0], list(agg_dict.values())[0]

            for k in agg_dict:
                for i in range(len(agg_dict[k].split(','))):
                    strip_string = agg_dict[k].replace(' ', '').split(',')
                    agg_item = spark_round(SparkMethods()[strip_string[i]](k), rnd).alias(f'{strip_string[i]}_{k}')
                    agg_list.append(agg_item)

            if rename_col:
                group_by_expr = col(group_by).alias(f'{group_by}_{pivot_col}')
            else:
                group_by_expr = col(group_by).alias(f'{group_by}')

            if len(list(agg_dict)) == 1:
                pivot_df = self.groupBy(group_by_expr).pivot(pivot_col).agg(*agg_list).repartition(rep).na.fill(0)

                if rename_col:
                    renamed_cols = [col(c).alias(f"{c}_{keys}_{values}") for c in pivot_df.columns[1:]]
                    return pivot_df.select(f'{group_by}_{pivot_col}', *renamed_cols)
                else:
                    renamed_cols = [col(c).alias(f"{c}") for c in pivot_df.columns[1:]]
                    return pivot_df.select(f'{group_by}', *renamed_cols)

            return self.groupBy(group_by_expr).pivot(pivot_col).agg(*agg_list).repartition(rep).na.fill(0)
        except TypeError as te:
            raise OpheliaFunctionsException(f"An error occurred while calling wide_format() method: {te}")


class ReshapeWrapper(DataFrame):

    DataFrame.toWide = Reshape.wide_format
    DataFrame.toNarrow = Reshape.narrow_format


class PctChange:

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
        function = PctChange.__build_pct_change_function(x=pct_cols, t=periods, w=win)
        return self.select(function)

    @staticmethod
    def __infer_sort_column(self):
        regex_match = ['year', 'Year', 'date', 'Date', 'month', 'Month', 'day', 'Day']
        regex_list = self.selectRegex(regex_expr(regex_match)).columns
        f_pick = feature_pick(self)
        if len(regex_list) > 0:
            return regex_list
        elif len(f_pick['date']) > 0:
            return f_pick['date']
        else:
            return f_pick['string']

    @staticmethod
    def __infer_lag_column(self):
        f_pick = feature_pick(self)
        if len(f_pick['double']) > 0:
            return f_pick['double']
        elif len(f_pick['float']) > 0:
            return f_pick['float']
        elif len(f_pick['long']) > 0:
            return f_pick['long']
        else:
            return f_pick['int']

    @staticmethod
    def pct_change(self, periods=1, order_by=None, pct_cols=None, partition_by=None, desc=None):
        if (order_by is None) and (pct_cols is None):
            infer_sort = PctChange.__infer_sort_column(self)[0]
            infer_lag = PctChange.__infer_lag_column(self)[1]
            return PctChange.__build_window_partition(self, periods, infer_sort, infer_lag)
        return PctChange.__build_window_partition(self, periods, order_by, pct_cols, partition_by, desc)

    @staticmethod
    def remove_element(self, col_remove):
        primary_list = self.columns
        [primary_list.remove(c) for c in col_remove]
        return primary_list


class PctChangeWrapper(DataFrame):

    DataFrame.pctChange = PctChange.pct_change
    DataFrame.remove_element = PctChange.remove_element


class CrossTabular:

    @staticmethod
    def __expression(cols_list, xpr):
        expr_dict = {
            'sum': '+'.join(cols_list),
            'sub': '-'.join(cols_list),
            'mul': '*'.join(cols_list),
            'div': '/'.join(cols_list),
        }
        return expr_dict[xpr]

    @staticmethod
    def foreach_col(self, group_by, pivot_col, agg_dict, oper):
        func = []
        regex_keys = list(agg_dict.keys())
        regex_values = list(agg_dict.values())
        df = self.toWide(group_by, pivot_col, agg_dict)
        for i in range(len(regex_keys)):
            cols_list = df.selectRegex(regex_expr(regex_keys[i])).columns
            expression = expr(CrossTabular.__expression(cols_list, oper))
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
        sum_by_col_df = CrossTabular.foreach_col(self, group_by, pivot_col, agg_dict, oper)
        return sum_by_col_df.union(CrossTabular.resume_dataframe(sum_by_col_df, new_col=self.columns[0]))

    @staticmethod
    def cross_pct(self, group_by, pivot_col, agg_dict, operand='sum', cols=None):
        sum_by_col_df = CrossTabular.foreach_col(self, group_by, pivot_col, agg_dict, operand)
        union_df = sum_by_col_df.union(CrossTabular.resume_dataframe(sum_by_col_df, new_col=self.columns[0]))
        key_list = list(agg_dict.keys())
        func = []
        for i in range(len(key_list)):
            tmp_df = sum_by_col_df.selectRegex(regex_expr([key_list[i]]))
            fix_df = tmp_df.selectRegex(regex_expr([operand]))
            pivot_list = tmp_df.drop(fix_df.columns[0]).columns
            for ix in range(len(pivot_list)):
                operate_cols = [pivot_list[ix], fix_df.columns[0]]
                dynamic_expr = CrossTabular.__expression(operate_cols, 'div')
                func.append(spark_round(expr(dynamic_expr), 4).alias(f'{pivot_list[ix]}_prop'))
        if cols == 'all':
            return union_df.select('*', *func)
        else:
            return union_df.select(f'{group_by}_{pivot_col}', *func)


class CrossTabularWrapper(DataFrame):

    DataFrame.foreachCol = CrossTabular.foreach_col
    DataFrame.resumeDF = CrossTabular.resume_dataframe
    DataFrame.tabularTable = CrossTabular.tab_table
    DataFrame.crossPct = CrossTabular.cross_pct


class Joins:

    @staticmethod
    def join_small_df(self, small_df, on, how):
        """
        Join Small wrapper broadcasts small sized DataFrames generating a copy of the same
        DataFrame in every worker.
        :param self: heritage DataFrame class object
        :param small_df: refers to the small size DataFrame to copy
        :param on: str for the join column name or a list of Columns
        :param how: str default 'inner'. Must be one of: {'inner', 'cross', 'outer',
        'full', 'fullouter', 'full_outer', 'left', 'leftouter', 'left_outer',
        'right', 'rightouter', 'right_outer', 'semi', 'leftsemi', 'left_semi',
        'anti', 'leftanti', 'left_anti'}
        :return

        Example:
        ' >>> big_df.join_small(small_df, on='On_Var', how='left') '
        """
        return self.join(broadcast(small_df), on, how)


class JoinsWrapper(DataFrame):

    DataFrame.joinSmall = Joins.join_small_df


class DaskSpark:

    @staticmethod
    def __spark_to_dask(self):
        """
        TODO: Se necesita optimizar la manera en la que se escribe con coalesce(1) se debe escribir con Spark Streaming
        """
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


class DaskSparkWrapper(DataFrame):

    DataFrame.toPandasSeries = DaskSpark.spark_to_series
    DataFrame.toNumpyArray = DaskSpark.spark_to_numpy
