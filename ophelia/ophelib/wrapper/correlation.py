import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

__all__ = ["CorrDF", "CorrDFWrapper"]


class CorrDF(object):
    
    @staticmethod
    def __corr(pair):
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
    def cartesian_rdd(self, default_pivot: str, rep: int = 20):
        if default_pivot is None:
            raise ValueError("'default_pivot' must be specified")
        rep_df = self.repartition(rep)
        numerical_cols = rep_df.columns[1:]
        to_wide_rdd = rep_df.rdd.map(lambda x: (x[default_pivot], [x[c] for c in numerical_cols]))
        return to_wide_rdd.cartesian(to_wide_rdd)

    @staticmethod
    def correlation_matrix(self, pivot_col: str = None, method: str = 'pearson', offset: float = 0.7, rep: int = 20):
        default_pivot = pivot_col if pivot_col is not None else self.columns[0]
        cartesian_rdd = CorrDF.cartesian_rdd(self, default_pivot=default_pivot, rep=rep)
        new_col_list = [f'{default_pivot}_m_dim', f'{default_pivot}_n_dim', f'{method}_coeff']
        corr_df = cartesian_rdd.map(CorrDF.__corr).toDF(schema=new_col_list)
        offset_condition = when(col(f'{method}_coeff') >= offset, lit(1.0)).otherwise(0.0)
        corr_label = CorrDF.__build_corr_label(method)
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
        matrix_df = self.toMatrix(group_by=group_by, pivot_col=pivot_col, agg_dict=agg_dict)
        vec_df = matrix_df.vecAssembler(matrix_df.columns[1:])
        corr_test = Correlation.corr(vec_df, 'features', method)\
            .select(col(f'{method}(features)').alias(f'{method}_features'))
        corr_cols = [f"{method}_{c}_{b}" for c in categories_list for b in categories_list]
        extract = (lambda row: tuple(float(x) for x in row[f'{method}_features'].values))
        return corr_test.rdd.map(extract).toDF(corr_cols)


class CorrDFWrapper(object):

    DataFrame.uniqueRow = CorrDF.unique_row
    DataFrame.cartRDD = CorrDF.cartesian_rdd
    DataFrame.corrMatrix = CorrDF.correlation_matrix
    DataFrame.vecAssembler = CorrDF.vector_assembler
    DataFrame.corrStat = CorrDF.spark_correlation
