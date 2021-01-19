import numpy as np
from math import log
from typing import List, AnyStr, Any
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.sql.types import Row
from pyspark.sql.dataframe import DataFrame
from ophelia.spark.logger import OpheliaLogger
from ophelia.spark import OpheliaMLMinerException

__all__ = ["FeatureMiner", "SparkFeatureMinerWrapper"]


class FeatureMiner(object):
    """
    ML Feature Miner is a class for static methods to help build the most common
    used methods for pyspark.ml data preprocessing, e.g.
    'StringIndexer', 'VectorAssembler', 'OneHotEncoder'
    """

    @staticmethod
    def single_string_indexer(single_col: AnyStr) -> StringIndexer:
        """
        Single string indexer method creates 'StringIndexer' pyspark.ml object
        :param single_col: str column name from str datatype column to index
        :return: StringIndexer
        """
        try:
            OpheliaLogger().info("Creating Single String Indexers")
            return StringIndexer(inputCol=single_col, outputCol="{COL}_index".format(COL=single_col))
        except TypeError as te:
            raise OpheliaMLMinerException(f"An error occurred while calling single_string_indexer() method: {te}")

    @staticmethod
    def multi_string_indexer(multi_col: List[AnyStr]) -> List[StringIndexer]:
        """
        Multi string indexer method creates a list of 'StringIndexer'
        pyspark.ml object by each given column
        :param multi_col: list for column string names to index
        :return: list of multi StringIndexer
        """
        try:
            OpheliaLogger().info("Creating Multi String Indexers")
            return [StringIndexer(
                inputCol=column,
                outputCol="{COLS}_index".format(COLS=column)) for column in multi_col]
        except TypeError as te:
            raise OpheliaMLMinerException(f"An error occurred while calling multi_string_indexer() method: {te}")

    @staticmethod
    def ohe_estimator(col_list: List[AnyStr]) -> OneHotEncoder:
        """
        One hot encoder estimator method creates 'OneHotEncoder' pyspark.ml object
        :param col_list: list for column string names to encode
        :return: OneHotEncoder
        """
        try:
            indexers = FeatureMiner.multi_string_indexer(multi_col=col_list)
            OpheliaLogger().info("Creating Feature Encoders")
            return OneHotEncoder(
                inputCols=[indexer.getOutputCol() for indexer in indexers],
                outputCols=["{COLS}_encoded".format(COLS=indexer.getOutputCol()) for indexer in indexers])
        except TypeError as te:
            raise OpheliaMLMinerException(f"An error occurred while calling ohe_estimator() method: {te}")

    @staticmethod
    def build_string_indexer(self, col_name: Any) -> DataFrame:
        """
        String Indexer builder wrapper for single and multi string indexing
        :param self: pyspark DataFrame to transform
        :param col_name: any column(s) name(s)
        :return: transformed spark DataFrame
        """
        if isinstance(col_name, list):
            pipe_ml = Pipeline(stages=[*FeatureMiner.multi_string_indexer(multi_col=col_name)])
        else:
            pipe_ml = Pipeline(stages=[FeatureMiner.single_string_indexer(single_col=col_name)])
        fit_model = pipe_ml.fit(dataset=self)
        OpheliaLogger().info("Build String Indexer")
        return fit_model.transform(self)

    @staticmethod
    def build_one_hot_encoder(self, col_name: List[AnyStr]) -> DataFrame:
        """
        One hot encoder builder wrapper for encoding numeric and categorical features
        :param self: pyspark DataFrame to transform
        :param col_name:
        :return: transformed spark DataFrame
        """
        encoder = Pipeline(stages=[FeatureMiner.ohe_estimator(col_name)])
        encode_vector = encoder.fit(dataset=self)
        OpheliaLogger().info("Build One Hot Encoder")
        return encode_vector.transform(self)

    @staticmethod
    def build_vector_assembler(self, input_cols: List[AnyStr], rename_vec_col: AnyStr = None) -> DataFrame:
        """
        Vector assembler builder wrapper for sparse and dense vectorization, only numeric features accepted
        :param self: pyspark DataFrame to transform
        :param input_cols:
        :param rename_vec_col:
        :return: transformed spark DataFrame
        """
        feature_col = 'features' if rename_vec_col is None else rename_vec_col
        vec_assembler = VectorAssembler(inputCols=input_cols, outputCol=feature_col)
        OpheliaLogger().info("Build Vector Assembler")
        return vec_assembler.transform(self)

    @staticmethod
    def spark_to_numpy(self, columns: List[AnyStr] = None) -> np.ndarray:
        """
        Spark to numpy converter from features of the DataFrame
        :param self: pyspark DataFrame to transform
        :param columns: list columns string name, optional
        :return: np.array with features
        """
        if columns is None:
            np_numbers = ['float', 'double', 'decimal', 'integer']
            columns = [k for k, v in self.dtypes if v in np_numbers]
        feature_df = FeatureMiner.build_vector_assembler(self, columns).select("features").cache()
        to_numpy = np.asarray(feature_df.rdd.map(lambda x: x[0]).collect())
        feature_df.unpersist()
        OpheliaLogger().info("Spark to Numpy Converter")
        return to_numpy

    @staticmethod
    def numpy_to_vector_assembler(numpy_array: np.ndarray, label_t: Any = 1) -> DataFrame:
        """
        Numpy to spark vector converter from a numpy object
        :param numpy_array: numpy array with features
        :param label_t: label type column, 1 as default
        :return: build from np.array to spark DataFrame
        """
        data_set = self._sc.parallelize(numpy_array)
        data_rdd = data_set.map(lambda x: (Row(features=DenseVector(x), label=label_t)))
        OpheliaLogger().info("Numpy to Spark Converter")
        return data_rdd.toDF()

    @staticmethod
    def probability_class(node):
        node_sum = sum(node.values())
        percents = {c: v / node_sum for c, v in node.items()}
        return node_sum, percents

    @staticmethod
    def gini_score(node):
        _, percents = FeatureMiner.probability_class(node)
        # donde i contiene la probabilidad calculada del nodo en cuestión
        score = round(1 - sum([i**2 for i in percents.values()]), 3)
        OpheliaLogger().info(f'Gini Score for node {node}: {score}')
        return score

    @staticmethod
    def entropy_score(node):
        _, percents = FeatureMiner.probability_class(node)
        score = round(sum([-i * log(i, 2) for i in percents.values()]), 3)
        OpheliaLogger().info(f'Entropy Score for node {node}: {score}')
        return score

    @staticmethod
    def information_gain(parent, children, criterion):
        score = {'gini': FeatureMiner.gini_score, 'entropy': FeatureMiner.entropy_score}
        metric = score[criterion](parent)
        parent_score = metric
        parent_sum = sum(parent.values())
        weighted_child_score = sum([metric(i) * sum(i.values()) / parent_sum for i in children])
        gain = round((parent_score - weighted_child_score), 2)
        OpheliaLogger().info(f'Information gain: {gain}')
        return gain


class SparkFeatureMinerWrapper(object):

    DataFrame.toVecAssembler = FeatureMiner.build_vector_assembler
    DataFrame.toOHEncoder = FeatureMiner.build_one_hot_encoder
    DataFrame.toStringIndex = FeatureMiner.build_string_indexer
    DataFrame.toNumpy = FeatureMiner.spark_to_numpy
