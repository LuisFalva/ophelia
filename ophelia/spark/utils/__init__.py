from ophelia.spark.utils.list_utils import ListUtils
from ophelia.spark.utils.rdd_utils import RDDUtils
from ophelia.spark.utils.df_utils import DataFrameUtils


def regex_expr(regex_name):
    return ListUtils.regex_expr(regex_name)


def feature_pick(df):
    return ListUtils.feature_pick(df)


def remove_duplicate_element(lst):
    return ListUtils.remove_duplicate_element(lst)


class OpheliaUtils(object):

    __all__ = ["ListUtils", "RDDUtils", "DataFrameUtils", "regex_expr", "feature_pick",
               "remove_duplicate_element", "OpheliaUtils"]
