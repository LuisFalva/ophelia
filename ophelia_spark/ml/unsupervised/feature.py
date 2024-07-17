import numpy as np
from pyspark.ml import Transformer
from pyspark.ml.feature import PCA, PCAModel
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.mllib.util import MLUtils
from pyspark.sql.functions import monotonically_increasing_id, udf

from ophelia_spark import OpheliaMLException
from ophelia_spark.ml.feature_miner import BuildStandardScaler, BuildVectorAssembler

from ..._logger import OpheliaLogger


class PCAnalysis(Transformer):
    """
    for PCA algorithm
    """

    def __init__(self, k=None, metadata_path=None):
        super().__init__()
        self.__k = k
        self.__metadata_path = metadata_path

    def __build_pca(self, df, metadata_path):
        pca = PCA(k=self.__k, inputCol="scaled_features", outputCol="pca_features")
        if self.__metadata_path:
            pca.fit(df).write().overwrite().save(metadata_path)
            return PCAModel.load(metadata_path).transform(df)
        return pca.fit(df).transform(df)

    def _transform(self, dataset):
        return self.__build_pca(dataset, self.__metadata_path)


class SingularVD(Transformer):
    """
    for SVD algorithm
    """

    def __init__(self, k=None, offset=95, label_col="label"):
        super().__init__()
        self.__logger = OpheliaLogger()
        self.k = k
        self.offset = offset
        self.label_col = label_col

    def __to_dense_vector(self, vec_df):
        """

        :param vec_df:
        :return:
        """
        try:
            to_dense_udf = udf(lambda v: Vectors.dense(v.toArray()), VectorUDT())
            self.__logger.info("Parsing Feature Vector To DenseUDT")
            return vec_df.select(
                monotonically_increasing_id().alias("id"),
                to_dense_udf("features").alias("features"),
            )
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling __to_dense_vector() method: {te}"
            )

    def __standard_features(
        self,
        df,
        with_mean=True,
        with_std=True,
        model_path="data/master/ophelia_spark/out/model/save/StandardScalerModel",
    ):
        """
        Standardizes features by removing the mean and scaling to unit variance using column summary
        statistics on the samples in the training set and persisting the estimator into local root FS,
        in order to change this please set your own path. This also may work over NFS and HDFS as well.
        :param df: spark DataFrame to re-scale to a standard scale
        :param with_mean: bool to indicate compute mean corrected sample, True as default
        :param with_std: bool to indicate compute unit std corrected sample standard deviation, True as default
        :param model_path: path were estimator model will be saved
        :return: spark DataFrame with Standard Scaled features
        """
        try:
            vector_assemble_df = BuildVectorAssembler(
                df.drop(self.label_col).colmuns
            ).transform(df)
            dense_vec_df = self.__to_dense_vector(vector_assemble_df)
            self.__logger.info("Compute Feature Standard Scaler")
            if model_path is not None:
                return BuildStandardScaler(with_mean, with_std, model_path).transform(
                    dense_vec_df
                )
            return BuildStandardScaler(with_mean, with_std).transform(dense_vec_df)
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling __standard_features() method: {te}"
            )

    def __index_row_matrix_rdd(self, scale_df):
        """

        :param scale_df:
        :return:
        """
        try:
            vector_mllib = MLUtils.convertVectorColumnsFromML(
                scale_df, "scaled_features"
            ).drop("features")
            vector_rdd = vector_mllib.select("scaled_features", "id").rdd.map(
                lambda x: IndexedRow(x[1], x[0])
            )
            self.__logger.info("Build Index Row Matrix RDD")
            return IndexedRowMatrix(vector_rdd)
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling __index_row_matrix_rdd() method: {te}"
            )

    @staticmethod
    def find_k(var_array, offset=95):
        """

        :param var_array:
        :param offset:
        :return:
        """
        try:
            return np.argmax(var_array > offset) + 1
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling find_k() method: {te}"
            )

    def svd_set_dict(self, df, d, tot_row, k, compute_u=True):
        """

        :param df:
        :param d:
        :param tot_row:
        :param k:
        :param compute_u:
        :return:
        """
        try:
            if k is None:
                k = d
                self.__logger.warning(
                    f"Warning: Set automatic 'K' == d == {d} since parameter is not specified"
                )
            scaled_df = self.__standard_features(df)
            svd_rdd = self.__index_row_matrix_rdd(scaled_df).computeSVD(
                k=k, computeU=compute_u
            )
            self.__logger.warning(f"Compute SVD With K={k}, d={d}, n={tot_row}")
            return {
                "svd": svd_rdd,
                "U": svd_rdd.U,
                "S": svd_rdd.s.toArray(),
                "V": svd_rdd.V,
                "n": tot_row,
                "d": d,
            }
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling svd_set_dict() method: {te}"
            )

    def compute_eigenvalues(self, svd):
        """

        :param svd:
        :return:
        """
        try:
            self.__logger.info("Compute Eigenvalues & Eigenvectors From Hyperplane")
            return np.flipud(np.sort(svd["S"] ** 2 / (svd["n"] - 1)))
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling compute_eigenvalues() method: {te}"
            )

    def cumulative_variance(self, eigen_vals):
        """
        Cumulative variance method will compute the cumulative variance array foreach component,
        each element inside the array will describe the number of components to choose in order
        to get the desired cumulative variance explained from original data
        :param eigen_vals: numpy array with eigenvalues
        :return: numpy array with total variance for each component
        """
        try:
            self.__logger.info("Compute Variance For Each Component")
            return (eigen_vals.cumsum() / eigen_vals.sum()) * 100
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling cumulative_variance() method: {te}"
            )

    def __foreach_pc_var_print(self, search_func, tot_var, var_list=(75, 85, 95)):
        """

        :param search_func:
        :param tot_var:
        :param var_list:
        :return:
        """
        try:
            message = (
                lambda var: f"Components For {var}% Variance K={search_func(tot_var, var)}"
            )
            self.__logger.info(f"Cumulative Variance Array:\n{tot_var}")
            for v in var_list:
                self.__logger.warning(message(var=v))
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling __foreach_pc_var_print() method: {te}"
            )

    def optimal_pc(self, svd_set_dict, offset):
        """

        :param svd_set_dict:
        :param offset:
        :return:
        """
        try:
            tot_var = self.cumulative_variance(self.compute_eigenvalues(svd_set_dict))
            optimal_k = self.find_k(tot_var, offset=offset)
            self.__logger.info(f"Find K={optimal_k} Components For {offset}% Variance")
            self.__foreach_pc_var_print(self.find_k, tot_var)
            self.__logger.info(f"Fit Optimal PCA Model With K={optimal_k} Components")
            return PCAnalysis(
                optimal_k, "data/master/ophelia_spark/out/model/save/OptimalPCAModel"
            )
        except TypeError as te:
            raise OpheliaMLException(
                f"An error occurred while calling optimal_pc() method: {te}"
            )

    def operate(self, df):
        svd_dict = self.svd_set_dict(
            df=df, d=len(df.columns), tot_row=df.count(), k=self.k
        )
        return self.optimal_pc(svd_set_dict=svd_dict, offset=self.offset)

    def _transform(self, dataset):
        return self.operate(df=dataset)


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
