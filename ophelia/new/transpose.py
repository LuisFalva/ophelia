from pyspark.sql import DataFrame
from pyspark.sql.functions import max as spark_max, min as spark_min
from pyspark.sql.functions import mean, col, first, explode, struct, lit, array, count


class TransposeDF:

    @staticmethod
    def add_transpose():
        def panel_format(self, pivot_col, new_columns: list = []):
            first_col = new_columns[0]
            second_col = new_columns[1]
            piv_col = [pivot_col]
            df_types = self.dtypes
            cols, dtype = zip(*[(c, t) for (c, t) in df_types if c not in piv_col])
            if len(set(dtype)) > 1:
                raise ValueError("Columns are not the same data type...")
            generator_explode = explode(array([
                struct(lit(c).alias(first_col), col(c).alias(second_col)) for c in cols
            ])).alias("column_explode")
            column_to_explode = ["column_explode."+first_col, "column_explode."+second_col]
            panel_df = self.select(piv_col + [generator_explode])\
                           .select(piv_col + column_to_explode)
            return panel_df
        DataFrame.T = panel_format
        DataFrame.transpose = panel_format
        DataFrame.toPanel = panel_format

    @staticmethod
    def add_to_matrix():
        def panel_to_matrix(self, group_by, pivot_col, agg_dict):
            agg_type_dict = {
                'first': first,
                'count': count,
                'max': spark_max,
                'min': spark_min,
                'mean': mean
            }
            agg_list = [agg_type_dict[agg_dict[k]](k) for k in agg_dict]
            return self.groupBy(group_by).pivot(pivot_col).agg(*agg_list)
        DataFrame.toMatrix = panel_to_matrix
