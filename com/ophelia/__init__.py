from typing import Any
from types import MethodType
from pyspark.sql.context import SQLContext, HiveContext, UDFRegistration
from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.catalog import Catalog
from pyspark.sql.dataframe import DataFrame, DataFrameNaFunctions, DataFrameStatFunctions
from pyspark.sql.group import GroupedData
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql.window import Window, WindowSpec
from pyspark.sql.types import DataType, NullType, BinaryType, IntegerType, BooleanType, DateType, TimestampType, \
    LongType, StructType, StructField, StringType, DoubleType, FloatType, ArrayType, DecimalType, ByteType, \
    ShortType, MapType, Row
from com.ophelia.OpheliaMask import OpheliaMask
from com.ophelia.OpheliaTools import ListUtils, ParseUtils, DataFrameUtils, RDDUtils
from com.ophelia.session.OpheliaSpark import OpheliaSpark
from com.ophelia.read.Read import Read
from com.ophelia.write.Write import Write
from enquire.engine.OpheliaClassifier import OpheliaClassifier


def ClassType(dtype):
    return dtype.__class__


def ClassName(dtype):
    return dtype.__class__.__name__


class OpheliaToolsException(Exception):
    """
    Ophelia Tools Exception
    """
    pass


class OpheliaMLException(Exception):
    """
    Ophelia ML Exception
    """
    pass


class OpheliaEvaluatorException(Exception):
    """
    Ophelia Evaluator Exception
    """
    pass


class OpheliaReadException(Exception):
    """
    Ophelia Read Exception
    """
    pass


class OpheliaSessionException(Exception):
    """
    Ophelia Session Exception
    """
    pass


class OpheliaStudioException(Exception):
    """
    Ophelia Studio Exception
    """
    pass


class OpheliaUtilsException(Exception):
    """
    Ophelia Utils Exception
    """
    pass


class OpheliaWrapperException(Exception):
    """
    Ophelia Wrapper Exception
    """
    pass


class OpheliaWriteException(Exception):
    """
    Ophelia Write Exception
    """
    pass


class OpheliaSQL:

    __all__ = [Row, SQLContext, HiveContext, UDFRegistration, SparkSession, Column, Catalog, DataFrame,
               DataFrameNaFunctions, DataFrameStatFunctions, GroupedData, DataFrameReader, DataFrameWriter,
               Window, WindowSpec, DateType, NullType, BinaryType, TimestampType, MapType, IntegerType, BooleanType,
               DataType, LongType, StructField, StructType, StringType, DoubleType, FloatType, ArrayType, DecimalType,
               ByteType, ShortType, MethodType, Any]


class OpheliaFeatures:

    __all__ = [OpheliaMask, ListUtils, ParseUtils, DataFrameUtils, RDDUtils, OpheliaSpark,
               Read, Write, OpheliaClassifier]


class Path:
    root = "data"
    dir = "ophelia"
    out = "out"
    load = "load"
    engine = "engine"
    model = "model"
    process_type = [engine, model]


BuildLoad = (
    lambda opt, project:
    Path.root + "/" +
    Path.dir + "/" +
    Path.load + "/" +
    opt + "/" +
    project + "/"
)
BuildOut = (
    lambda opt, project:
    Path.root + "/" +
    Path.dir + "/" +
    Path.out + "/" +
    opt + "/" +
    project + "/"
)
