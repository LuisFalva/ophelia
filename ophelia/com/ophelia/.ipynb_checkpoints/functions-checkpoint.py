from pyspark.sql.functions import col, year, month, dayofmonth, udf
from pyspark.sql.types import IntegerType, LongType, StructType, StructField
from pyspark.sql import Row


class Arrays(object):
    
    @staticmethod
    def year_array(from_year, to_year):
        """
        METHOD DESCRIPTION
        :param [PARAM]: [DESCRIPTION]
        :return: [RETURN TYPE]
        """
        from_year_param, to_year_param = from_year, to_year
        year_array = list(range(int(from_year_param), int(to_year_param)+1))
        print("-Ophelia[INFO]: Set Year Parameters Array", year_array, "[...]")
        print("===="*18)
        return year_array
    
    @staticmethod
    def dates_index(dates_list):
        """
        Dates parser function, transform a list of dates in a dictionary
        :param dates_list: list with date values
        :return: parser udf for sequence of dates
        """
        if not isinstance(dates_list, list):
            print("-Ophelia[FAIL]: Invalid Parameters Array", dates_list, "[...]")

        if len(dates_list) <= 0:
            print("-Ophelia[FAIL]: Empty Parameters Array", dates_list, "[...]")

        print("-Ophelia[INFO]: Set Date Index [...]")
        dates_dict = {date: index for index, date in enumerate(dates_list)}
        result = udf(lambda x: dates_dict[x], IntegerType())
        print("-Ophelia[INFO]: Set Date Index Successfully [...]")
        print("===="*18)
        return result
    
    @staticmethod
    def sorted_date_list(df, col_collect):
        """
        METHOD DESCRIPTION
        :param [PARAM]: [DESCRIPTION]
        :return: [RETURN TYPE]
        """
        dates_list = sorted([x.operation_date for x in df.select(col_collect).distinct().collect()])
        return dates_list
    
    @staticmethod
    def feature_picking(dataframe):
        """
        METHOD DESCRIPTION
        :param [PARAM]: [DESCRIPTION]
        :return: [RETURN TYPE]
        """
        columns = dataframe.columns
        categorical = []
        numerical = []
        decimal = []
        date = []
        
        for i in range(len(columns)):
            if dataframe.dtypes[i][1] == "string":
                categorical.append(columns[i])
            elif dataframe.dtypes[i][1] == "int":
                numerical.append(columns[i])
            elif dataframe.dtypes[i][1] == "float":
                decimal.append(columns[i])
            else:
                date.append(columns[i])

        print("-Ophelia[INFO]: Feature Picking Quite Well [...]")
        print("-Ophelia[WARN]: You Must Choose Between Them {'string', 'int', 'float', 'date'} [...]")
        print("===="*18)
        return {"string": categorical, "int": numerical, "float": decimal, "date": date}    


class Parse(object):
    
    @staticmethod
    def str_to_date(string):
        pass


class DataFrame(object):
    
    @staticmethod
    def split_date_columns(df, col_date):
        """
        METHOD DESCRIPTION
        :param [PARAM]: [DESCRIPTION]
        :return: [RETURN TYPE]
        """
        dates_df = df.select('*', year(col_date).alias(str(col_date)+'_year'),
                             month(col_date).alias(str(col_date)+'_month'),
                             dayofmonth(col_date).alias(str(col_date)+'_day'))
        
        print("-Ophelia[INFO]: Split Dates In Columns [...]")
        print("===="*18)
        return dates_df
    
    @staticmethod
    def lag_min_max_data(df, is_max=True, is_min=False, col_lag="operation_date"):
        """
        METHOD DESCRIPTION
        :param [PARAM]: [DESCRIPTION]
        :return: [RETURN TYPE]
        """
        if is_max == True:
            lag_date = max(df.select(col_lag).distinct().collect())[0]
        elif is_min == True:
            lag_date = min(df.select(col_lag).distinct().collect())[0]

        lag_data = df.where(col(col_lag) < lag_date).select([col(c).alias("{0}_lag".format(c)) for c in df.columns])
        print("-Ophelia[INFO]: Lag-Over Dates In Dataframe [...]")
        print("===="*18)
        return lag_data
    

class RDD(object):
    
    @staticmethod
    def row_indexing(df, desc=True, sort_by="operation_date"):
        """
        METHOD DESCRIPTION
        :param [PARAM]: [DESCRIPTION]
        :return: [RETURN TYPE]
        """
        if desc == True:
            df = df.orderBy(col(sort_by).desc())
        else:
            df = df.orderBy(col(sort_by))
        schema  = StructType(df.schema.fields[:] + [StructField(name=sort_by[:9]+"_index", dataType=LongType(), nullable=False)])
        row_indexation = Row()
        rdd_index = df.rdd.zipWithIndex()
        
        print("-Ophelia[INFO]: Indexing Row RDD [...]")
        indexed_df = rdd_index.map(lambda row: row_indexation(*list(row[0]) + [row[1]])).toDF(schema)
        print("-Ophelia[INFO]: Indexing Row RDD Quite Good [...]")
        print("===="*18)
        return indexed_df