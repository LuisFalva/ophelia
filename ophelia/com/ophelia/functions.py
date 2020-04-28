from pyspark.sql.functions import col, year, month, dayofmonth, udf, row_number, desc, asc
from pyspark.sql.types import IntegerType, LongType, StructType, StructField
from pyspark.sql.window import Window
from pyspark.sql import Row
from com.ophelia.utils.logger import OpheliaLogger


logger = OpheliaLogger()


class Arrays(object):
    
    @staticmethod
    def year_array(from_year, to_year):
        """
        Gets every year number between a range, including the upper limit

        :param from_year: start year number
        :param to_year: end year number
        :return: list
        """
        from_year_param, to_year_param = from_year, to_year
        year_array = list(range(int(from_year_param), int(to_year_param)+1))
        logger.info("Set Year Parameters Array " + str(year_array))
        return year_array
    
    @staticmethod
    def dates_index(dates_list):
        """
        Dates parser function, transform a list of dates in a dictionary

        :param dates_list: sequence of date values
        :return: function
        """
        if not isinstance(dates_list, list):
            raise TypeError("Invalid Parameters Array")
        elif len(dates_list) == 0:
            raise ValueError("Empty Parameters Array")
        logger.info("Set Date Index...")
        dates_dict = {date: index for index, date in enumerate(dates_list)}
        result = udf(lambda x: dates_dict[x], IntegerType())
        logger.info("Set Date Index Successfully...")
        return result
    
    @staticmethod
    def sorted_date_list(df, col_collect):
        """
        Builds a sorted list of every value for a date column in a given DataFrame

        :param df: data to analyze
        :param col_collect: column to analyze
        :return: list
        """
        dates_list = sorted([x.operation_date for x in df.select(col_collect).distinct().collect()])
        return dates_list
    
    @staticmethod
    def feature_picking(df):
        """
        This is the placeholder for a definition of this method

        :param df: data to analyze
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
        logger.info("Feature Picking Quite Well...")
        logger.warning("You Must Choose Between Them {'string', 'int', 'float', 'date'}...")
        return {"string": categorical, "int": numerical, "float": decimal, "date": date}    


class Parse(object):
    
    @staticmethod
    def str_to_date(string):
        pass


class DataFrame(object):
    
    @staticmethod
    def split_date_columns(df, col_date):
        """
        This is the placeholder for a description

        :param df: data to analyze
        :param col_date: column with date to analyze
        :return: DataFrame
        """
        dates_df = df.select('*', year(col_date).alias(str(col_date)+'_year'),
                             month(col_date).alias(str(col_date)+'_month'),
                             dayofmonth(col_date).alias(str(col_date)+'_day'))
        logger.info("Split Dates In Columns...")
        return dates_df
    
    @staticmethod
    def row_index(df, col_order):
        """
        This is a placeholder for this method

        :param df: data to analyze
        :param col_order: column to order
        :return: DataFrame
        """
        w = Window().orderBy(col(col_order).desc())
        return df.withColumn("row_num", row_number().over(w))
    
    @staticmethod
    def lag_min_max_data(df, is_max=True, col_lag="operation_date"):
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
        logger.info("Lag-Over Dates In Dataframe...")
        return lag_data
    

class RDD(object):

    @staticmethod
    def row_indexing(data, sort_by="operation_date", is_desc=True):
        """
        Adds the index of every row as a new column for a given DataFrame

        :param data: data to index
        :param sort_by: name of the column to sort
        :param is_desc: indicate if you need the data sorted in ascending or descending order
        :return: DataFrame
        """
        data = data.orderBy(desc(sort_by)) if is_desc else data.orderBy(asc(sort_by))
        field_name = sort_by.split("_")[0] + "_index"
        schema = StructType(data.schema.fields + [StructField(name=field_name, dataType=LongType(), nullable=False)])
        rdd_index = data.rdd.zipWithIndex()
        logger.info("Indexing Row RDD...")
        indexed_df = rdd_index.map(lambda row: Row(*list(row[0]) + [row[1]])).toDF(schema)
        logger.info("Indexing Row RDD Quite Good...")
        return indexed_df
