from com.ophelib.wrapper.shape import ShapeDF, ShapeDFWrapper
from com.ophelib.wrapper.transpose import TransposeDF, TransposeDFWrapper
from com.ophelib.wrapper.pct_change import PctChangeDF, PctChangeWrapper
from com.ophelib.wrapper.correlation import CorrDF, CorrDFWrapper
from com.ophelib.wrapper.dask_spark import DaskSpark, DaskSparkWrapper
from com.ophelib.wrapper.joins import Joins, JoinsWrapper
from com.ophelib.wrapper.selects import Selects, SelectWrapper
from com.ophelib.wrapper.whens import Whens
from com.ophelib.wrapper.cross_tabs import DynamicPivot, DynamicPivotWrapper
from com.ophelib.wrapper.sampling import DynamicSampling, DynamicSamplingWrapper
from com.ophelib.wrapper.rolling import Rollings, RollingsWrapper


def string_match(string_condition):
    return Whens.string_match(string_condition)


def union_all(dfs):
    return DynamicSampling.union_all(dfs)


def regex_expr(regex_name):
    return Selects.regex_expr(regex_name)


class SparkWrapper(object):

    __all__ = ["ShapeDF", "ShapeDFWrapper", "TransposeDF", "TransposeDFWrapper",
               "PctChangeDF", "PctChangeWrapper", "CorrDF", "CorrDFWrapper",
               "DaskSpark", "DaskSparkWrapper", "Joins", "Selects", "SelectWrapper",
               "DynamicPivot", "DynamicPivotWrapper", "DynamicSampling", "DynamicSamplingWrapper",
               "Rollings", "RollingsWrapper", "string_match", "union_all", "regex_expr"]
