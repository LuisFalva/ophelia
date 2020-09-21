from new.shape import ShapeDF
from new.transpose import TransposeDF
from new.pct_change import PctChangeDF
from new.correlation import CorrDF
from new.dask_spark import DaskSpark

ShapeDF.add_shape()
TransposeDF.add_transpose()
TransposeDF.add_to_matrix()
PctChangeDF.add_pct_change()
PctChangeDF.add_remove_element()
CorrDF.add_corr()
DaskSpark.add_to_series()
DaskSpark.add_to_numpy()
