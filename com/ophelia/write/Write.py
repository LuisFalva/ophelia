from pyspark.sql import DataFrame
from com.ophelia.utils.logger import OpheliaLogger
from com.ophelia.write import PathWrite, WritePath


class Write:

    __logger = OpheliaLogger()
    __path = PathWrite()

    @staticmethod
    def write_parquet(df: DataFrame, output_type: str, project: str,
                      part: str = None, mode: str = None, rep: int = None) -> str:
        """
        Read file function helps to read multi-format file in spark
        :param df: spark DataFrame to write in parquet
        :param output_type: str, output name type for data-warehouse
        :param project: str, project name output data
        :param part: str, partition name field
        :param mode: str, spark writer mode
        :param rep: int, number of parts in each partition
        :return: spark DataFrame
        """
        if output_type not in [Write.__path.model, Write.__path.engine]:
            Write.__logger.error("output '" + output_type + "' type not allowed")
            raise ValueError("you must choose 'engine' or 'model' for output type")
        else:
            path = WritePath(output_type, project)
            mode = "overwrite" if not mode else mode
            rep = 5 if not rep else rep
            partition = [part]
            if not part:
                Write.__logger.info("Write Parquet")
                df.coalesce(rep).write.mode(mode).parquet(path)
                Write.__logger.info("Write Parquet Successfully")
                Write.__logger.warning("Parquet Parts With No Name Partition...")
                Write.__logger.info("New Parquet Path: " + path)
                return path
            Write.__logger.info("Write Parquet")
            df.coalesce(rep).write.mode(mode).parquet(path, partitionBy=partition)
            Write.__logger.info("Write Parquet Successfully")
            Write.__logger.info("New Parquet Path: " + path + part + "=yyy-MM-dd")
            return path

    @staticmethod
    def write_json(df: DataFrame, output_type: str, project: str,
                   part: str = None, mode: str = None, rep: int = None) -> str:
        path = WritePath(output_type, project)
        df.repartition(rep).write.mode(mode).partitionBy(part).json(path)
        Write.__logger.info("Write Json")
        return path

    @staticmethod
    def write_csv(df: DataFrame, output_type: str, project: str,
                  part: str = None, mode: str = None, rep: int = None) -> str:
        path = WritePath(output_type, project)
        df.repartition(rep).write.mode(mode).partitionBy(part).csv(path)
        Write.__logger.info("Write CSV")
        return path
