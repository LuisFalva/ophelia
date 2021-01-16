from ophelia.spark.functions.shape import ShapeDF, ShapeDFWrapper
from ophelia.spark.functions.transpose import TransposeDataFrame, TransposeDataFrameWrapper
from ophelia.spark.functions.pct_change import PctChangeDF, PctChangeWrapper
from ophelia.spark.functions.correlation import CorrDF, CorrDFWrapper
from ophelia.spark.functions.dask_spark import DaskSpark, DaskSparkWrapper
from ophelia.spark.functions.joins import Joins, JoinsWrapper
from ophelia.spark.functions.selects import Selects, SelectWrapper
from ophelia.spark.functions.whens import Whens
from ophelia.spark.functions.cross_tabs import DynamicPivot, DynamicPivotWrapper
from ophelia.spark.functions.sampling import DynamicSampling, DynamicSamplingWrapper
from ophelia.spark.functions.rolling import Rollings, RollingsWrapper


def string_match(string_condition):
    return Whens.string_match(string_condition)


def union_all(dfs):
    return DynamicSampling.union_all(dfs)


def regex_expr(regex_name):
    return Selects.regex_expr(regex_name)


class OpheliaWrappers(object):

    __all__ = ["ShapeDF", "ShapeDFWrapper", "TransposeDataFrame", "TransposeDataFrameWrapper",
               "PctChangeDF", "PctChangeWrapper", "CorrDF", "CorrDFWrapper", "DaskSpark",
               "DaskSparkWrapper", "Joins", "JoinsWrapper", "Selects", "SelectWrapper", "DynamicPivot",
               "DynamicPivotWrapper", "DynamicSampling", "DynamicSamplingWrapper", "Rollings", "RollingsWrapper",
               "string_match", "union_all", "regex_expr"]
