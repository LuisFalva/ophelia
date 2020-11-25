from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, explode, struct, array, round as spark_round
from ophelia.ophelib.wrapper.rolling import Rollings

__all__ = ["TransposeDF", "TransposeDFWrapper"]


class TransposeDF(object):

    @staticmethod
    def panel_format(self, pivot_col, new_col=None):
        if new_col is None:
            new_col = ['no_name_first_col', 'no_name_new_col']
        if isinstance(new_col, list):
            first_col = new_col[0]
            alias_new_col = new_col[1]
        else:
            first_col = self.columns[0].split('_')[1]
            alias_new_col = new_col
        piv_col = [pivot_col]
        cols, dtype = zip(*[(c, t) for (c, t) in self.dtypes if c not in piv_col])
        #if len(set(map(lambda x: x[-1], df.dtypes[1:]))) != 1:
        #    raise AssertionError("Columns must be of the same datatype")
        generator_explode = explode(array([
            struct(lit(c).alias(first_col), col(c).alias(alias_new_col)) for c in cols
        ])).alias('column_explode')
        column_to_explode = [f'column_explode.{first_col}', f'column_explode.{alias_new_col}']
        panel_df = self.select([pivot_col] + [generator_explode]) \
                       .select([pivot_col] + column_to_explode)
        return panel_df

    @staticmethod
    def matrix_format(self, group_by, pivot_col, agg_dict, rnd=4, rep=20):
        keys = list(agg_dict.keys())[0]
        values = list(agg_dict.values())[0]
        methods = Rollings.spark_methods()
        agg_list = []
        for k in agg_dict:
            for i in range(len(agg_dict[k].split(','))):
                strip_string = agg_dict[k].replace(' ', '').split(',')
                agg_list.append(spark_round(methods[strip_string[i]](k), rnd).alias(f'{strip_string[i]}_{k}'))
        group_by_expr = col(group_by).alias(f'{group_by}_{pivot_col}')
        if len(list(agg_dict)) == 1:
            pivot_df = self.groupBy(group_by_expr).pivot(pivot_col).agg(*agg_list).repartition(rep).na.fill(0)
            renamed_cols = [col(c).alias(f"{c}_{keys}_{values}") for c in pivot_df.columns[1:]]
            return pivot_df.select(f'{group_by}_{pivot_col}', *renamed_cols)
        return self.groupBy(group_by_expr).pivot(pivot_col).agg(*agg_list).repartition(rep).na.fill(0)


class TransposeDFWrapper(object):

    DataFrame.toMatrix = TransposeDF.matrix_format
    DataFrame.toPanel = TransposeDF.panel_format
