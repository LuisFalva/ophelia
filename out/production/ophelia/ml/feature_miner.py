import numpy as np
from math import log
from typing import List, AnyStr, Any
from pyspark.sql.types import Row
from pyspark.sql.dataframe import DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder, StandardScaler
from ophelia.ophelia.ophelia_logger import OpheliaLogger
from ophelia.ophelia import OpheliaMLMinerException

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
        :param self: Spark DataFrame object auto-reference from DataFrame class
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
    def build_one_hot_encoder(self, col_name: List[AnyStr], persist_estimator_path: AnyStr = None):
        """
        One hot encoder builder wrapper for encoding numeric and categorical features
        :param self: Spark DataFrame object auto-reference from DataFrame class
        :param col_name: Column names to encode
        :param persist_estimator_path: Persist model estimator metadata path
        :return: transformed spark DataFrame
        """
        encoder = Pipeline(stages=[FeatureMiner.ohe_estimator(col_name)])
        if persist_estimator_path:
            OpheliaLogger().info("Build One Hot Encoder Estimator Model Metadata")
            OpheliaLogger().warning(f"Persist Metadata Model Path: {persist_estimator_path}")
            return encoder.fit(dataset=self).write().overwrite().save(persist_estimator_path)
        else:
            OpheliaLogger().info("Build One Hot Encoder Estimator DataFrame")
            return encoder.fit(dataset=self).transform(self)

    @staticmethod
    def build_vector_assembler(self, input_cols: List[AnyStr], rename_vec_col: AnyStr = None) -> DataFrame:
        """
        Vector assembler builder wrapper for sparse and dense vectorization, only numeric features accepted
        :param self: Spark DataFrame object auto-reference from DataFrame class
        :param input_cols:
        :param rename_vec_col: Set new name for vector transformation
        :return: Vector Assembler Transformation
        """
        feature_col = 'features' if rename_vec_col is None else rename_vec_col
        vec_assembler = VectorAssembler(inputCols=input_cols, outputCol=feature_col)
        OpheliaLogger().info("Build Vector Assembler")
        return vec_assembler.transform(self)

    @staticmethod
    def build_standard_scaler(self, with_mean: bool = True, with_std: bool = True, persist_model_path: AnyStr = None,
                              input_col: AnyStr = 'features', output_col: AnyStr = 'scaled_features'):
        """
        Standard Scaler estimator builder and transformer for dense feature vectors.
        Warnings: It will build a dense output, so take care when applying to sparse input.
        :param self: Spark DataFrame object auto-reference from DataFrame class
        :param with_mean: False by default. Centers the data with mean before scaling
        :param with_std: True by default. Scales the data to unit standard deviation
        :param persist_model_path: Persist model metadata path
        :param input_col: Name for input column to scale
        :param output_col: Name of output column to create with scaled features
        :return: Standard Scaler model
        """
        std_scaler = StandardScaler(
            withMean=with_mean, withStd=with_std, inputCol=input_col, outputCol=output_col
        )
        if persist_model_path:
            OpheliaLogger().info("Compute Feature Standard ScalerModel Metadata")
            OpheliaLogger().info(f"Persist Metadata Model Path: {persist_model_path}")
            return std_scaler.fit(self).write().overwrite().save(persist_model_path)
        else:
            OpheliaLogger().info("Compute Feature Standard Scaler DataFrame")
            return std_scaler.fit(self).transform(self)

    @staticmethod
    def spark_to_numpy(self, columns: List[AnyStr] = None) -> np.ndarray:
        """
        Spark to numpy converter from features of the DataFrame
        :param self: Spark DataFrame object auto-reference from DataFrame class
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
        data_set = _sc.parallelize(numpy_array)
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
