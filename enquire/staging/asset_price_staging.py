import pandas as pd
from pyspark.sql.functions import col, to_date, current_date, current_timestamp
from enquire.staging.module import ModuleConstants
from ophelia.spark.OpheliaMain import Ophelia


class AssetPriceStaging(object):

    def __init__(self, input_path, sheet_name, output_path, rep=10, mode='overwrite', part=True):
        self.input_path = input_path
        self.sheet_name = sheet_name
        self.output_path = output_path
        self.rep = rep
        self.mode = mode
        self.part = part
        self.cons = ModuleConstants()
        self.spark = Ophelia('AssetPriceStaging').SparkSession

    def create_spark_df(self):
        pd_df = pd.read_excel(self.input_path, self.sheet_name)
        return self.spark.createDataFrame(pd_df)

    def __replace_char(self, char):
        return char.lower().replace(' ', '_').replace(':_0', '').replace('unnamed', self.cons.information_timestamp)

    def rename_columns(self, df):
        try:
            rename_col = df.select(*[col(c).alias(self.__replace_char(c)) for c in df.columns])\
                .select('*',
                        to_date(self.cons.information_timestamp).alias(self.cons.information_date),
                        current_date().alias(self.cons.process_date),
                        current_timestamp().alias(self.cons.process_timestamp))
            return rename_col
        finally:
            return df.select(*[col(c).alias(self.__replace_char(c)) for c in df.columns])

    def write_staging_parquet(self, df):
        if self.part is False:
            df.repartition(self.rep).write.mode(self.mode).parquet(self.output_path)
        else:
            df.repartition(self.rep).write.mode(self.mode).parquet(self.output_path,
                                                                   partitionBy=self.cons.information_date)

    def run(self):
        spark_df = self.create_spark_df()
        rename_df = self.rename_columns(spark_df)
        self.write_staging_parquet(rename_df)
