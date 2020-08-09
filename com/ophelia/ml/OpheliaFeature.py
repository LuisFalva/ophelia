from pyspark.sql.dataframe import DataFrame
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoderEstimator
from com.ophelia import ClassName, ClassType
from com.ophelia.ml import OpheliaMLObjects


class OpheliaFeature:

    @staticmethod
    def pipe(transform_list: list) -> Pipeline:
        return Pipeline(stages=transform_list)

    @staticmethod
    def fit(df: DataFrame, pipe):
        if ClassType(pipe) not in OpheliaMLObjects.__all__:
            raise TypeError("'pipe' must be a 'OpheliaMLObjects' not " + ClassName(pipe))
        return pipe.fit(df)

    @staticmethod
    def transform(df: DataFrame, model) -> DataFrame:
        if ClassType(model) not in OpheliaMLObjects.__all__:
            raise TypeError("'model' must be a 'OpheliaMLObjects' not " + ClassName(model))
        return model.transform(df)

    @staticmethod
    def single_string_indexer(single_col: str) -> StringIndexer:
        return StringIndexer(inputCol=single_col, outputCol=single_col + "_index")

    @staticmethod
    def multi_string_indexer(multi_col: list) -> list:
        indexer = [StringIndexer(
            inputCol=column,
            outputCol="{0}_index".format(column)) for column in multi_col]
        return indexer

    @staticmethod
    def ohe_estimator(col_list: list) -> OneHotEncoderEstimator:
        indexers = OpheliaFeature.multi_string_indexer(multi_col=col_list)
        encoder = OneHotEncoderEstimator(
            inputCols=[indexer.getOutputCol() for indexer in indexers],
            outputCols=["{0}_encoded".format(indexer.getOutputCol()) for indexer in indexers])
        return encoder

    @staticmethod
    def indexer_encoded(index_list: list) -> list:
        multi_indexers = OpheliaFeature.multi_string_indexer(index_list)
        encode_index_list = []
        for column in range(len(multi_indexers)):
            encode_index_list.append(multi_indexers[column].getOutputCol() + "_encoded")
        return encode_index_list

    @staticmethod
    def vector_assembler(column_names: list) -> VectorAssembler:
        return VectorAssembler(inputCols=column_names, outputCol='features')
