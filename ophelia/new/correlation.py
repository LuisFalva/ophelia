import pandas as pd
from pyspark.sql import DataFrame


class CorrDF:
    
    @staticmethod
    def __corr(pair):
        (prod1, series1), (prod2, series2) = pair
        corr = pd.Series(series1).corr(pd.Series(series2))
        return prod1, prod2, float(corr)

    @staticmethod
    def add_corr():
        def corr(self, pivot_col=None):
            default_pivot = pivot_col if pivot_col is not None else self.columns[0]
            numerical_cols = self.columns[1:]
            to_wide_rdd = self.rdd.map(lambda x: (x[default_pivot], [x[c] for c in numerical_cols]))
            cartesian_rdd = to_wide_rdd.cartesian(to_wide_rdd)
            new_col_list = ['{}_m_dim'.format(default_pivot), '{}_n_dim'.format(default_pivot), 'pearson_coefficient']
            return cartesian_rdd.map(CorrDF.__corr).toDF(schema=new_col_list)
        DataFrame.corr = corr
        DataFrame.toCorrelationMatrix = corr
