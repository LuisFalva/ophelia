from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count as spark_count, avg as spark_avg
from com.ophelia.OpheliaVendata import OpheliaVendata
from transfromer.OpheliaModule import Configuration


class OpheliaTransformer:

    conf = Configuration()

    def __init__(self):
        self.ophelia = OpheliaVendata()
        self.ophelia_read = self.ophelia.ophelia_read
        self.ophelia_array = self.ophelia.ophelia_array
        self.ophelia_df = self.ophelia.ophelia_df

    def read_portfolio_data(self, path_file: str, source: str, date_col: str, with_schema: bool = True) -> DataFrame:
        spark = self.ophelia.ophelia_spark
        portfolio_data = self.ophelia_read.read_file(spark, path_file, source)
        if with_schema:
            return self.ophelia_read.build_portfolio_schema(portfolio_data, date_col)
        return portfolio_data

    def build_portfolio_data(self, path_file: str, source: str, date_col: str) -> DataFrame:
        portfolio_df = OpheliaTransformer.read_portfolio_data(
            self,
            path_file=path_file,
            source=source,
            date_col=date_col,
        )
        return portfolio_df

    def portfolio_date_window(self, df: DataFrame, col_date: str, from_year, to_year) -> DataFrame:
        year_array = self.ophelia_array.year_array(from_year, to_year)
        split_dates = self.ophelia_df.split_date(df, col_date)
        operation_dates_list = self.ophelia_array.sorted_date_list(df, col_date)
        date_index_udf = self.ophelia_array.dates_index(operation_dates_list)
        portfolio_dates = split_dates.where(col(col_date+"_year").isin(year_array))\
            .select('*', (date_index_udf(col(col_date))).alias(col_date[:9]+"_id"))
        return portfolio_dates

    def build_portfolio_window(self) -> DataFrame:
        portfolio_window_df = OpheliaTransformer.portfolio_date_window(
            self, df=OpheliaTransformer.build_portfolio_data(
                self, Configuration.path,
                Configuration.source,
                Configuration.col_date
            ),
            from_year=Configuration.from_year,
            to_year=Configuration.to_year,
            col_date=Configuration.col_date
        )
        return portfolio_window_df

    def monitoring_empty_vector(self, df: DataFrame, feature_type: str) -> DataFrame:
        float_cols = self.ophelia_array.feature_picking(df)[feature_type]
        count_by_col = [spark_count(col(x)).alias(str(x)) for x in float_cols]
        aggregate_columns = df.select(*count_by_col)
        return aggregate_columns

    @staticmethod
    def debug_null(panel: DataFrame, missing_days: int, n: int) -> list:
        null_count = panel.select([col(c).alias(c) for c in panel.columns]).collect()[0].asDict()
        clean_null_list = [k for k, v in null_count.items() if v < abs(missing_days - n)]
        return clean_null_list

    def debug_empty_vector(self, df: DataFrame, feature_type: str, missing_days: int = conf.max_miss_day) -> DataFrame:
        sample_count = df.count()
        empty_panel = OpheliaTransformer.monitoring_empty_vector(self, df, feature_type)
        clean_null_list = OpheliaTransformer.debug_null(empty_panel, missing_days, sample_count)
        debug_vector = df.drop(*clean_null_list)
        return debug_vector

    def mean_impute(self, df: DataFrame) -> DataFrame:
        float_cols = self.ophelia_array.feature_picking(df)["float"]
        numerical_fields = df.agg(*(spark_avg(c).alias(c) for c in df.columns if c in float_cols))
        portfolio_base_table = df.na.fill(numerical_fields.collect()[0].asDict())
        return portfolio_base_table

    def build_portfolio_base_table(self) -> DataFrame:
        remove_none_df = OpheliaTransformer \
            .debug_empty_vector(self, OpheliaTransformer.build_portfolio_window(self), feature_type="float")
        portfolio_base_table = OpheliaTransformer.mean_impute(self, remove_none_df) \
            .drop("operation_date_year", "operation_date_month", "operation_date_day")
        return portfolio_base_table
