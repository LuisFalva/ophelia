from pyspark.sql.functions import broadcast
from pyspark.sql import DataFrame

__all__ = ["Joins", "JoinsWrapper"]


class Joins(object):

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


class JoinsWrapper(object):

    DataFrame.joinSmall = Joins.join_small_df
