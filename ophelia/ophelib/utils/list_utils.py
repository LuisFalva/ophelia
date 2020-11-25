from typing import Dict, Callable
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from ophelia import InstanceError
from ophelia.ophelib.utils.logger import OpheliaLogger


class ListUtils:

    def __init__(self):
        self.__logger = OpheliaLogger()

    @staticmethod
    def regex_expr(regex_name):
        if isinstance(regex_name, list):
            return [f".*{regx}" for regx in regex_name]
        return [f".*{regex_name}"]

    @staticmethod
    def remove_duplicated_elements(lst):
        InstanceError(lst, list)
        return list(dict.fromkeys(lst))

    def year_array(self, from_year, to_year) -> list:
        """
        Gets every year number between a range, including the upper limit
        :param from_year: start year number
        :param to_year: end year number
        :return: list
        """
        year_array = list(range(int(from_year), int(to_year) + 1))
        self.__logger.info("Set Year Parameters Array " + str(year_array))
        return year_array

    def dates_index(self, dates_list: list) -> Callable:
        """
        Dates parser function, transform a list of dates in a dictionary
        :param dates_list: sequence of date values
        :return: callable function
        """
        if len(dates_list) == 0:
            raise ValueError("Empty Parameters Array")
        dates_dict = {date: index for index, date in enumerate(dates_list)}
        result = udf(lambda x: dates_dict[x], IntegerType())
        self.__logger.info("Set Date Index Successfully")
        return result

    def sorted_date_list(self, df: DataFrame, col_collect: str) -> list:
        """
        Builds a sorted list of every value for a date column in a given DataFrame
        :param df: data to analyze
        :param col_collect: column to analyze
        :return: list
        """
        dates_list = sorted([x.operation_date for x in df.select(col_collect).distinct().collect()])
        self.__logger.info("Order Date List")
        return dates_list

    @staticmethod
    def feature_picking(df: DataFrame) -> Dict[str, list]:
        """
        Feature picking function helps to split variable names from spark DataFrame
        into 'string', 'int', 'bigint', 'double', 'float', 'date' and 'other' type in separated list
        :param df: spark DataFrame with fields to analyze
        :return: dict
        """
        s = []
        i = []
        l = []
        d = []
        f = []
        t = []
        o = []
        for col_id in range(len(df.columns)):
            s.append(df.columns[col_id]) if df.dtypes[col_id][1] == "string" else \
                i.append(df.columns[col_id]) if df.dtypes[col_id][1] == "int" else \
                l.append(df.columns[col_id]) if df.dtypes[col_id][1] == "bigint" else \
                d.append(df.columns[col_id]) if df.dtypes[col_id][1] == "double" else \
                f.append(df.columns[col_id]) if df.dtypes[col_id][1] == "float" else \
                t.append(df.columns[col_id]) if df.dtypes[col_id][1] == "date" else \
                o.append(df.columns[col_id])
        return {"string": s, "int": i, "long": l, "double": d, "float": f, "date": t, "other": o}

    def __binary_helper_search(self, array, target, left_p, right_p):
        if left_p > right_p:
            self.__logger.error("Binary Null")
            return -1
        mid_point = (left_p + right_p) // 2
        potential_match = array[mid_point]
        if target == potential_match:
            return mid_point
        elif target < potential_match:
            return self.__binary_helper_search(array, target, left_p, mid_point - 1)
        else:
            return self.__binary_helper_search(array, target, mid_point + 1, right_p)

    def binary_search(self, array, target):
        self.__logger.info("Binary Find")
        return self.__binary_helper_search(array, target, 0, len(array) - 1)
