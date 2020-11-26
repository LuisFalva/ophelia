import pandas as pd
from pyspark.sql.functions import col, to_date, year
from ophelia.ophelib.OpheliaMain import Ophelia


class AssetPriceStaging(object):

    def __init__(self, input_path, sheet_name, output_path, rep=10, mode='overwrite', part=None):
        self.input_path = input_path
        self.sheet_name = sheet_name
        self.rep = rep
        self.mode = mode
        self.part = part
        self.output_path = output_path
        self.spark = Ophelia('AssetPriceStaging').SparkSession

    def create_spark_df(self):
        pd_df = pd.read_excel(self.input_path, self.sheet_name)
        return self.spark.createDataFrame(pd_df)

    @staticmethod
    def __replace_char(char):
        return char.lower().replace(' ', '_').replace(':_0', '').replace('unnamed', 'close_timestamp')

    @staticmethod
    def rename_columns(df):
        try:
            rename_col = df.select(*[col(c).alias(AssetPriceStaging.__replace_char(c)) for c in df.columns])\
                .select('*', to_date('close_timestamp').alias('close_date'),
                        year('close_timestamp').alias('close_year'))
            return rename_col
        finally:
            return df.select(*[col(c).alias(AssetPriceStaging.__replace_char(c)) for c in df.columns])

    def write_staging_parquet(self, df):
        if self.part is None:
            df.repartition(self.rep).write.mode(self.mode).parquet(self.output_path)
        else:
            df.repartition(self.rep).write.mode(self.mode).parquet(self.output_path, partitionBy=self.part)

    def run(self):
        spark_df = self.create_spark_df()
        rename_df = self.rename_columns(spark_df)
        self.write_staging_parquet(rename_df)
