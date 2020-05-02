from pyspark.sql.functions import date_format, to_date, col

class Transform(object):
    """
    """
    @staticmethod
    def read_file(path_source=None, source=None, spark_session=None):
        """
        Class object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"
        """
        if path_source is None:
            print("-Ophelia[FAIL]: Please, set path source argument [...]")
            return None
        if source is None:
            print("-Ophelia[FAIL]: Please, set source file argument [...]")
            return None
        if spark_session is None:
            print("-Ophelia[FAIL]: Please, set spark session argument [...]")
            return None
        if source == "parquet":
            print("-Ophelia[INFO]: Reading Spark File [...]")
            file_df = spark_session.read.parquet(path_source)
            print("-Ophelia[INFO]: Read Parquet Successfully From Path:", path_source, "[...]")
            print("===="*18)
            return file_df
        if source == "csv":
            print("-Ophelia[INFO]: Reading Spark File [...]")
            file_df = spark_session.read.csv(path_source, header = True, inferSchema = True)
            print("-Ophelia[INFO]: Read CSV Successfully From Path:", path_source, "[...]")
            print("===="*18)
            return file_df
        
        print("-Ophelia[INFO]: Reading Spark File [...]")
        file_df = spark_session.read.format(source).options(header="true").load(path_source)
        print("-Ophelia[INFO]: Read Format Successfully From Path:", path_source, "[...]")
        print("===="*18)
        return file_df
    
    @staticmethod
    def write_parquet(dataframe, name_directory, partition_field, mode="overwrite", repartition=1):
        partition = [partition_field]
        path = "data/master/ophelia/data/OpheliaData/"+str(name_directory)+"/"
        print("-Ophelia[INFO]: Writing Parquet [...]")
        dataframe.repartition(int(repartition)).write.mode(mode).parquet(path, partitionBy=partition)
        print("-Ophelia[INFO]: Writing Root Parquet Successfully [...]")
        print("-Ophelia[INFO]: Parquet Parts Partitioned By:", partition,"[...]")
        print("-Ophelia[INFO]: This is Your New Parquet Path:", path+"operation_date=yyy-MM-dd [...]")
        print("===="*18)
        return str(path)
    
    @staticmethod
    def schema_define(df, date_col):
        """
        Constructor object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"
        """
        print("-Ophelia[INFO]: Creating Schema File [...]")
        schema_portfolio = [date_format(
            to_date(col(df.columns[0]), 'dd/MM/yyyy'),
            "yyyy-MM-dd").cast("date").alias(date_col)] + [col(x).cast('float') for x in df.columns[1:]]

        schema_df = df.where(col(df.columns[0]).isNotNull())\
                      .select(schema_portfolio)
        print("-Ophelia[INFO]: Schema Define Successfully [...]")
        print("===="*18)
        return schema_df