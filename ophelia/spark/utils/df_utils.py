from pyspark.sql.functions import col, year, month, dayofmonth, row_number
from pyspark.sql import DataFrame, Window
from ophelia.spark import OpheliaDataFrameUtilsException
from ophelia.spark.utils.logger import OpheliaLogger


class DataFrameUtils:

    def __init__(self):
        self.__logger = OpheliaLogger()

    def split_date(self, df: DataFrame, col_date: str) -> DataFrame:
        """
        Split date helps to divide date field into day, month and year by each column
        :param df: spark DataFrame with date field to split
        :param col_date: str, column name date
        :return: spark DataFrame
        """
        try:
            dates_df = df.select('*', year(col_date).alias(f'{col_date}_year'),
                                 month(col_date).alias(f'{col_date}_month'),
                                 dayofmonth(col_date).alias(f'{col_date}_day'))
            self.__logger.info("Split Date In Columns")
            return dates_df
        except Exception as e:
            raise OpheliaDataFrameUtilsException(f"An error occurred on split_date() method: {e}")

    def row_index(self, df: DataFrame, col_order: str) -> DataFrame:
        """
        Row index method will help to create a row index for a given spark DataFrame
        :param df: data to analyze
        :param col_order: column to order
        :return: DataFrame
        """
        try:
            w = Window().orderBy(col(col_order).desc())
            self.__logger.info("Row Indexing In DataFrame")
            return df.withColumn("row_num", row_number().over(w))
        except Exception as e:
            raise OpheliaDataFrameUtilsException(f"An error occurred on row_index() method: {e}")

    def lag_min_max_data(self, df: DataFrame, is_max: bool = True, col_lag: str = "operation_date") -> DataFrame:
        """
        This is a placeholder for this method
        :param df: data to analyze
        :param is_max: indicates if it is max
        :param col_lag: name of the column to lag
        :return: DataFrame
        """
        try:
            if is_max:
                lag_date = max(df.select(col_lag).distinct().collect())[0]
            else:
                lag_date = min(df.select(col_lag).distinct().collect())[0]
            lag_data = df.where(col(col_lag) < lag_date).select([col(c).alias(f'{c}_lag') for c in df.columns])
            self.__logger.info("Lag-Over Dates In DataFrame")
            return lag_data
        except Exception as e:
            raise OpheliaDataFrameUtilsException(f"An error occurred on lag_min_max_data() method: {e}")
