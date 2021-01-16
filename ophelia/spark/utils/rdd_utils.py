from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, asc
from pyspark.sql.types import LongType, StructField, StructType, Row
from ophelia.spark import OpheliaRDDUtilsException
from ophelia.spark.utils.logger import OpheliaLogger


class RDDUtils:

    def __init__(self):
        self.__logger = OpheliaLogger()

    def row_indexing(self, data: DataFrame, sort_by: str, is_desc: bool = True) -> DataFrame:
        """
        Adds the index of every row as a new column for a given DataFrame
        :param data: df, data to index
        :param sort_by: str, name of the column to sort
        :param is_desc: bool, indicate if you need the data sorted in ascending or descending order
        :return: DataFrame
        """
        try:
            data = data.orderBy(desc(sort_by)) if is_desc else data.orderBy(asc(sort_by))
            field_name = sort_by.split("_")[0] + "_index"
            schema = StructType(
                data.schema.fields + [
                    StructField(name=field_name, dataType=LongType(), nullable=False)
                ]
            )
            self.__logger.info("Indexing Row RDD")
            return data.rdd.zipWithIndex().map(lambda row: Row(*list(row[0]) + [row[1]])).toDF(schema)
        except Exception as e:
            raise OpheliaRDDUtilsException(f"An error occurred on row_indexing() method: {e}")
