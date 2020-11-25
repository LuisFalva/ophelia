from typing import List
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder


class FeatureEngine:

    @staticmethod
    def single_string_indexer(single_col: str) -> StringIndexer:
        return StringIndexer(inputCol=single_col, outputCol="{0}_index".format(single_col))

    @staticmethod
    def multi_string_indexer(multi_col: List[str]) -> List[StringIndexer]:
        indexer = [StringIndexer(
            inputCol=column,
            outputCol="{0}_index".format(column)) for column in multi_col]
        return indexer

    @staticmethod
    def ohe_estimator(col_list: list) -> OneHotEncoder:
        indexers = FeatureEngine.multi_string_indexer(multi_col=col_list)
        encoder = OneHotEncoder(
            inputCols=[indexer.getOutputCol() for indexer in indexers],
            outputCols=["{0}_encoded".format(indexer.getOutputCol()) for indexer in indexers])
        return encoder

    @staticmethod
    def indexer_encoded(index_list: list) -> list:
        multi_indexers = FeatureEngine.multi_string_indexer(index_list)
        encode_index_list = []
        for column in range(len(multi_indexers)):
            encode_index_list.append(multi_indexers[column].getOutputCol() + "_encoded")
        return encode_index_list

    @staticmethod
    def vector_assembler(column_names: List[str]) -> VectorAssembler:
        return VectorAssembler(inputCols=column_names, outputCol='features')
