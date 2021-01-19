import numpy as np
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.ml import Transformer
from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.ml.feature import StandardScaler, PCA
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg.distributed import IndexedRowMatrix, IndexedRow
from ophelia.spark.ml.feature_miner import FeatureMiner
from ophelia.spark.logger import OpheliaLogger


class PrincipleComponent:
    """
    for PCA algorithm
    """


class SingularVD(Transformer):
    """
    for SVD algorithm
    """
    def __init__(self, k: int = None, offset: int = 95):
        super().__init__()
        self.__logger = OpheliaLogger()
        self.k = k
        self.offset = offset
        self.svd = None
        self.compute_u = None
        self.vec_df = None
        self.vec_scale_df = None

    def standard_features(self, df: DataFrame, with_mean: bool = True, with_std: bool = True) -> DataFrame:
        vector_assemble_df = FeatureMiner.build_vector_assembler(df, input_cols=df.columns)
        to_dense_udf = udf(lambda v: Vectors.dense(v.toArray()), VectorUDT())
        self.__logger.info(f"Parsing Feature Vector To DenseUDT")
        self.vec_df = vector_assemble_df.select(monotonically_increasing_id().alias('id'),
                                                to_dense_udf('features').alias('features'))
        std_scaler = StandardScaler(
            withMean=with_mean, withStd=with_std, inputCol='features', outputCol='scaled_features'
        )
        self.__logger.info("Compute Feature Standard Scaler")
        return std_scaler.fit(self.vec_df).transform(self.vec_df)

    def index_row_matrix_rdd(self, df: DataFrame) -> IndexedRowMatrix:
        self.vec_scale_df = self.standard_features(df)
        vector_mllib = MLUtils.convertVectorColumnsFromML(self.vec_scale_df, 'scaled_features').drop('features')
        self.__logger.info(f"Build Index RDD Row Matrix")
        return IndexedRowMatrix(vector_mllib.select('scaled_features', 'id').rdd.map(lambda x: IndexedRow(x[1], x[0])))

    @staticmethod
    def find_k(var_array: np.ndarray, offset: int = 95) -> np.int64:
        return np.argmax(var_array > offset) + 1

    def compute_svd(self, df: DataFrame, compute_u: bool = True) -> Dict:
        k, d = self.k, len(df.columns)
        if k is None:
            k = d
            self.__logger.warning(f"Warning: Set 'K' as d={k} since parameter is not specified")
        svd = self.index_row_matrix_rdd(df).computeSVD(k=k, computeU=compute_u)
        self.__logger.warning(f"Compute SVD With K={k}, d={d}, n={self.vec_df.count()}")
        return {'svd': svd, 'U': svd.U, 'S': svd.s.toArray(), 'V': svd.V, 'n': self.vec_df.count(), 'd': d}

    def __compute_pc(self, df: DataFrame) -> Dict:
        self.svd = self.compute_svd(df)

        self.__logger.info(f"Compute Eigenvalues & Eigenvectors From Hyperplane")
        eigen_val = np.flipud(np.sort(self.svd['S']**2 / (self.svd['n']-1)))

        self.__logger.info(f"Compute Variance For Each Component")
        tot_var = (eigen_val.cumsum() / eigen_val.sum()) * 100

        k_pc = self.find_k(tot_var, offset=self.offset)
        self.__logger.info(f"Find K={k_pc} Components For {self.offset}% Variance")
        self.compute_u = self.svd['U'].rows.map(lambda x: (x.index, x.vector[0:k_pc] * self.svd['S'][0:k_pc]))

        tape_message = (
            lambda variance:
            f"Components For {variance}% Variance K={self.find_k(tot_var, offset=variance)}"
        )
        self.__logger.info(f"Cumulative Variance Array:\n{tot_var}")
        self.__logger.warning(tape_message(variance=75))
        self.__logger.warning(tape_message(variance=85))
        self.__logger.warning(tape_message(variance=95))

        self.__logger.info(f"Fit Optimal PCA Model With K={k_pc} Components")
        pca = PCA(k=k_pc, inputCol='scaled_features', outputCol='pca_features')
        preorder_columns = ['id', 'features', 'scaled_features', 'pca_features']
        return pca.fit(self.vec_scale_df).transform(self.vec_scale_df).select(preorder_columns)

    def _transform(self, dataset: DataFrame):
        return self.__compute_pc(df=dataset)


class IndependentComponent:
    """
    for ICA algorithm
    """


class LinearDAnalysis:
    """
    for Linear Discriminant Analysis algorithm
    """


class LLinearEmbedding:
    """
    for Locally Linear Embedding Analysis algorithm
    """


class StochasticNeighbor:
    """
    for t-distributed Stochastic Neighbor Embedding (t-SNE) algorithm
    """
