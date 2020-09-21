from typing import List, Any
from com import ClassName, ClassType
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from com.ophelia.ml import OpheliaMLObjects


class Feature:

    def __init__(self):
        self.get_output_cols = None

    @staticmethod
    def pipe(transform_list: Any) -> Pipeline:
        if not isinstance(transform_list, list):
            pipe_wf = Pipeline(stages=[transform_list])
        else:
            pipe_wf = Pipeline(stages=transform_list)
        return pipe_wf

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
        return StringIndexer(inputCol=single_col, outputCol="{0}_index".format(single_col))

    @staticmethod
    def multi_string_indexer(multi_col: List[str]) -> List[StringIndexer]:
        indexer = [StringIndexer(
            inputCol=column,
            outputCol="{0}_index".format(column)) for column in multi_col]
        #self.get_output_cols = [index.getOutputCols() for index in indexer]
        return indexer

    @staticmethod
    def ohe_estimator(col_list: list) -> OneHotEncoder:
        indexers = Feature.multi_string_indexer(multi_col=col_list)
        encoder = OneHotEncoder(
            inputCols=[indexer.getOutputCol() for indexer in indexers],
            outputCols=["{0}_encoded".format(indexer.getOutputCol()) for indexer in indexers])
        return encoder

    @staticmethod
    def indexer_encoded(index_list: list) -> List:
        multi_indexers = Feature.multi_string_indexer(index_list)
        encode_index_list = []
        for column in range(len(multi_indexers)):
            encode_index_list.append(multi_indexers[column].getOutputCol() + "_encoded")
        return encode_index_list

    @staticmethod
    def vector_assembler(column_names: List[str]) -> VectorAssembler:
        return VectorAssembler(inputCols=column_names, outputCol='features')
