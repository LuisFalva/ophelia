from ophelia.ophelib.wrapper.shape import ShapeDF, ShapeDFWrapper
from ophelia.ophelib.wrapper.transpose import TransposeDF, TransposeDFWrapper
from ophelia.ophelib.wrapper.pct_change import PctChangeDF, PctChangeWrapper
from ophelia.ophelib.wrapper.correlation import CorrDF, CorrDFWrapper
from ophelia.ophelib.wrapper.dask_spark import DaskSpark, DaskSparkWrapper
from ophelia.ophelib.wrapper.joins import Joins, JoinsWrapper
from ophelia.ophelib.wrapper.selects import Selects, SelectWrapper
from ophelia.ophelib.wrapper.whens import Whens
from ophelia.ophelib.wrapper.cross_tabs import DynamicPivot, DynamicPivotWrapper
from ophelia.ophelib.wrapper.sampling import DynamicSampling, DynamicSamplingWrapper
from ophelia.ophelib.wrapper.rolling import Rollings, RollingsWrapper


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
