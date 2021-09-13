from pyspark.sql.functions import (
    count, first, grouping,
    mean, stddev, covar_pop,
    variance, coalesce,
    sum as spark_sum,
    min as spark_min,
    max as spark_max
)


__all__ = [
    "SparkMethods",
    "ClassType",
    "ClassName",
    "InstanceError",
    "FormatRead",
    "PathWrite",
    "FormatWrite",
    "OpheliaMLException",
    "OpheliaMLMinerException",
    "OpheliaUtilitiesException",
    "OpheliaReadFileException",
    "OpheliaSparkSessionException",
    "OpheliaSparkWrapperException",
    "OpheliaWriteFileException",
    "OpheliaFunctionsException"
]


def SparkMethods():
    return {
        'sum': spark_sum,
        'min': spark_min,
        'max': spark_max,
        'mean': mean,
        'stddev': stddev,
        'var': variance,
        'first': first,
        'count': count,
        'coalesce': coalesce,
        'covar_pop': covar_pop,
        'grouping': grouping
    }


def ClassType(dtype):
    return dtype.__class__


def ClassName(dtype):
    return dtype.__class__.__name__


def InstanceError(obj, t):
    if not isinstance(obj, t):
        raise TypeError("Unsupported Type {}".format(ClassName(obj)))
    return None


class FormatRead:

    def __init__(self):
        self.parquet = "parquet"
        self.excel = "excel"
        self.csv = "csv"
        self.json = "json"
        self.all = [self.parquet, self.excel, self.csv, self.json]


class FormatWrite:
    def __init__(self):
        self.parquet = "parquet"
        self.excel = "excel"
        self.csv = "csv"
        self.json = "json"
        self.all = [self.parquet, self.excel, self.csv, self.json]


class PathWrite:
    def __init__(self):
        self.root = "data"
        self.dir = "ophelia"
        self.out = "out"
        self.engine = "engine"
        self.model = "model"

    def WritePath(self, opt, project):
        return f"{self.root}/{self.dir}/{self.out}/{opt}/{project}/"


class OpheliaMLException(Exception):
    """
    Ophelia ML Exception
    """
    pass


class OpheliaMLMinerException(Exception):
    """
    Ophelia ML Miner Exception
    """
    pass


class OpheliaReadFileException(Exception):
    """
    Ophelia Read File Exception
    """
    pass


class OpheliaSparkSessionException(Exception):
    """
    Ophelia Spark Session Exception
    """
    pass


class OpheliaUtilitiesException(Exception):
    """
    Ophelia Utilities Exception
    """
    pass


class OpheliaSparkWrapperException(Exception):
    """
    Ophelia Spark Wrapper Exception
    """
    pass


class OpheliaWriteFileException(Exception):
    """
    Ophelia Write File Exception
    """
    pass


class OpheliaFunctionsException(Exception):
    """
    Ophelia Functions Exception
    """
    pass
