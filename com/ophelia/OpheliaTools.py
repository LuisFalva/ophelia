import numpy as np
import pandas as pd
import datetime as dt
from numpy import double
from typing import Dict, List, Callable
from pyspark.ml.linalg import DenseVector
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import IntegerType, LongType, StructField, StructType, Row
from pyspark.sql.functions import col, year, month, dayofmonth, udf, row_number, desc, asc
from com.ophelia.utils.logger import OpheliaLogger

logger = OpheliaLogger()


class ListUtils:

    @staticmethod
    def year_array(from_year, to_year) -> List:
        """
        Gets every year number between a range, including the upper limit
        :param from_year: start year number
        :param to_year: end year number
        :return: list
        """
        year_array = list(range(int(from_year), int(to_year) + 1))
        logger.info("Set Year Parameters Array " + str(year_array))
        return year_array

    @staticmethod
    def dates_index(dates_list: List) -> Callable:
        """
        Dates parser function, transform a list of dates in a dictionary
        :param dates_list: sequence of date values
        :return: callable function
        """
        if not isinstance(dates_list, List):
            raise TypeError("Invalid Parameters Array")
        elif len(dates_list) == 0:
            raise ValueError("Empty Parameters Array")
        dates_dict = {date: index for index, date in enumerate(dates_list)}
        result = udf(lambda x: dates_dict[x], IntegerType())
        logger.info("Set Date Index Successfully")
        return result

    @staticmethod
    def sorted_date_list(df: DataFrame, col_collect: str) -> List:
        """
        Builds a sorted list of every value for a date column in a given DataFrame
        :param df: data to analyze
        :param col_collect: column to analyze
        :return: list
        """
        logger.info("Order Date List")
        dates_list = sorted([x.operation_date for x in df.select(col_collect).distinct().collect()])
        return dates_list

    @staticmethod
    def feature_picking(df: DataFrame) -> Dict[str, List]:
        """
        Feature picking function helps to split variable names from spark DataFrame
        into 'string', 'int', 'float' and 'date' type in separated list
        :param df: spark DataFrame with fields to analyze
        :return: dict
        """
        columns = df.columns
        categorical = []
        numerical = []
        decimal = []
        date = []

        for i in range(len(columns)):
            categorical.append(columns[i]) if df.dtypes[i][1] == "string" else \
                numerical.append(columns[i]) if df.dtypes[i][1] == "int" else \
                decimal.append(columns[i]) if df.dtypes[i][1] == "float" else \
                date.append(columns[i])
        logger.info("Feature Picking Quite Well")
        logger.warning("Always Be Aware To Choose Between One Of Them {'string', 'int', 'float', 'date'}")
        return {"string": categorical, "int": numerical, "float": decimal, "date": date}


class ParseUtils:

    @staticmethod
    def str_to_date(string: str) -> dt.datetime:
        """
        string to date '%d/%m/%y %H:%M:%S'
        """
        return dt.datetime.strptime(string, '%d/%m/%y')

    @staticmethod
    def str_to_timestamp(string: str) -> dt.datetime:
        """
        string to date '%d/%m/%y %H:%M:%S'
        """
        return dt.datetime.strptime(string, '%d/%m/%y %H:%M:%S')

    @staticmethod
    def str_to_int(string: str) -> int:
        return int(string)

    @staticmethod
    def str_to_float(string: str) -> float:
        return float(string)

    @staticmethod
    def str_to_double(string: str) -> double:
        return double(string)

    @staticmethod
    def date_to_str(date: dt.date) -> str:
        return str(date)

    @staticmethod
    def int_to_str(integer: int) -> str:
        return str(integer)

    @staticmethod
    def int_to_float(integer: int) -> float:
        return float(integer)

    @staticmethod
    def int_to_double(integer: int) -> double:
        return double(integer)

    @staticmethod
    def int_to_date(y: int, m: int, d: int) -> dt.date:
        return dt.date(y, m, d)

    @staticmethod
    def float_to_str(floating: float) -> str:
        return str(floating)

    @staticmethod
    def float_to_int(floating: float) -> int:
        return int(floating)

    @staticmethod
    def float_to_double(floating: float) -> double:
        return double(floating)

    @staticmethod
    def spark_to_numpy(df, spark_session):
        cache_df = df.cache()
        to_numpy = np.asarray(cache_df.rdd.map(lambda x: x[0]).collect())
        spark_session.catalog.clearCache()
        return to_numpy

    @staticmethod
    def numpy_to_vector_assembler(numpy_array, label_t, spark_session):
        sc = spark_session.sparkContext
        data_set = sc.parallelize(numpy_array)
        data_rdd = data_set.map(lambda x: (Row(features=DenseVector(x), label=label_t)))
        return data_rdd.toDF()


class DataFrameUtils:

    @staticmethod
    def split_date(df: DataFrame, col_date: str) -> DataFrame:
        """
        Split date helps to divide date field into day, month and year by each column
        :param df: spark DataFrame with date field to split
        :param col_date: str, column name date
        :return: spark DataFrame
        """
        dates_df = df.select(
            '*',
            year(col_date).alias(str(col_date) + '_year'),
            month(col_date).alias(str(col_date) + '_month'),
            dayofmonth(col_date).alias(str(col_date) + '_day')
        )
        logger.info("Split Date In Columns")
        return dates_df

    @staticmethod
    def row_index(df: DataFrame, col_order: str) -> DataFrame:
        """
        Row index method will help to create a row index for a given spark DataFrame
        :param df: data to analyze
        :param col_order: column to order
        :return: DataFrame
        """
        w = Window().orderBy(col(col_order).desc())
        return df.withColumn("row_num", row_number().over(w))

    @staticmethod
    def lag_min_max_data(df: DataFrame, is_max: bool = True, col_lag: str = "operation_date") -> DataFrame:
        """
        This is a placeholder for this method
        :param df: data to analyze
        :param is_max: indicates if it is max
        :param col_lag: name of the column to lag
        :return: DataFrame
        """
        if is_max:
            lag_date = max(df.select(col_lag).distinct().collect())[0]
        else:
            lag_date = min(df.select(col_lag).distinct().collect())[0]
        lag_data = df.where(col(col_lag) < lag_date).select([col(c).alias("{0}_lag".format(c)) for c in df.columns])
        logger.info("Lag-Over Dates In DataFrame")
        return lag_data

    @staticmethod
    def __corr(pair):
        (prod1, series1), (prod2, series2) = pair
        corr = pd.Series(series1).corr(pd.Series(series2))
        return prod1, prod2, float(corr)

    @staticmethod
    def add_corr():
        def corr(self, pivot_col):
            numerical_cols = self.columns[1:]
            to_wide_rdd = self.rdd.map(lambda x: (x[pivot_col], [x[c] for c in numerical_cols]))
            cartesian_rdd = to_wide_rdd.cartesian(to_wide_rdd)
            new_col_list = [pivot_col+'_m_dim', pivot_col+'_n_dim', 'pearson_coefficient']
            return cartesian_rdd.map(DataFrameUtils.__corr).toDF(schema=new_col_list)
        DataFrame.corr = corr
        DataFrame.cart = corr


class RDDUtils:

    @staticmethod
    def row_indexing(data: DataFrame, sort_by: str, is_desc: bool = True) -> DataFrame:
        """
        Adds the index of every row as a new column for a given DataFrame
        :param data: df, data to index
        :param sort_by: str, name of the column to sort
        :param is_desc: bool, indicate if you need the data sorted in ascending or descending order
        :return: DataFrame
        """
        data = data.orderBy(desc(sort_by)) if is_desc else data.orderBy(asc(sort_by))
        field_name = sort_by.split("_")[0] + "_index"
        schema = StructType(
            data.schema.fields + [
                StructField(name=field_name, dataType=LongType(), nullable=False)
            ]
        )
        rdd_index = data.rdd.zipWithIndex()
        logger.info("Indexing Row RDD...")
        indexed_df = rdd_index.map(lambda row: Row(*list(row[0]) + [row[1]])).toDF(schema)
        logger.info("Indexing Row RDD Quite Good")
        return indexed_df
