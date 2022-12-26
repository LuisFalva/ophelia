from pyspark.ml.clustering import FuzzyKMeans, BisectingKMeans


class KMeansCluster:
    """
    traditional spark clustering
    """


class FuzzyClusterMeans:
    """
    FuzzyCMeans in ml.
    A class for performing fuzzy clustering using the FuzzyKMeans algorithm in PySpark.

    Parameters:
    data: pyspark.sql.DataFrame
        A PySpark DataFrame containing the data to be clustered.
    k: int
        The number of clusters to form.
    m: float
        The fuzzyness parameter, with a value between 1 and infinity.
    maxIter: int, optional (default=20)
        The maximum number of iterations to run the FuzzyKMeans algorithm.
    tol: float, optional (default=1e-4)
        The tolerance for the FuzzyKMeans algorithm, which is the maximum change in the sum of
        squared distances between the model and the data.

    Attributes:
    model: pyspark.ml.clustering.FuzzyKMeansModel
        The trained FuzzyKMeans model.
    """
    def __init__(self, data, k, m, max_iter=20, tol=1e-4):
        self.data = data
        self.k = k
        self.m = m
        self.max_iter = max_iter
        self.tol = tol

    def fit(self):
        """
        Fit the FuzzyKMeans model to the data.

        Returns:
        pyspark.ml.clustering.FuzzyKMeansModel:
            The trained FuzzyKMeans model.
        """
        fuzzy_kmeans = FuzzyKMeans(k=self.k, m=self.m, maxIter=self.max_iter, tol=self.tol)
        model = fuzzy_kmeans.fit(self.data)
        return model

    def transform(self, model, data):
        """
        Predict the cluster assignments for new data using the trained FuzzyKMeans model.

        Parameters:
        data: pyspark.sql.DataFrame
            A PySpark DataFrame containing the data to be transformed.

        Returns:
        pyspark.sql.DataFrame:
            A PySpark DataFrame containing the cluster assignments for each data point.
        """
        return model.transform(data)


class BKM:
    """
    A class for implementing the Bisecting K-Means (BKM) algorithm in PySpark.

    Parameters
    ----------
    k: int, optional (default=2)
        The number of clusters to create.
    max_iter: int, optional (default=20)
        The maximum number of iterations to run the BKM algorithm.
    seed: int, optional (default=None)
        The random seed to use.
    features_col: str, optional (default='features')
        The name of the column in the input data that contains the features to cluster.

    Attributes
    ----------
    model: pyspark.ml.clustering.BisectingKMeansModel
        The fitted BKM model.

    bkm = BKM(k=5)
    model = bkm.fit(data)
    clustered_data = bkm.transform(data)
    """
    def __init__(self, k=2, max_iter=20, seed=None, features_col='features'):
        self.k = k
        self.max_iter = max_iter
        self.seed = seed
        self.features_col = features_col
        self.model = None

    def fit(self, data):
        """
        Fit the BKM model to the input data.

        Parameters
        ----------
        data: pyspark.sql.DataFrame
            The input data to fit the model to.

        Returns
        -------
        model: pyspark.ml.clustering.BisectingKMeansModel
            The fitted BKM model.
        """
        bkm = BisectingKMeans(k=self.k, maxIter=self.max_iter, seed=self.seed, featuresCol=self.features_col)
        self.model = bkm.fit(data)
        return self.model

    def transform(self, data):
        """
        Apply the BKM model to the input data to create clusters.

        Parameters
        ----------
        data: pyspark.sql.DataFrame
            The input data to apply the model to.

        Returns
        -------
        data: pyspark.sql.DataFrame
            The input data with an additional column containing the predicted cluster for each data point.
        """
        return self.model.transform(data)

    def get_cluster_centers(self):
        return self.model.clusterCenters()

    def get_cluster_sizes(self):
        return self.model.summary.clusterSizes


class GaussianM:
    """
    traditional spark clustering
    """


class LatentDirichlet:
    """
    traditional spark clustering
    """


class ClusterFeatureTree:
    """
    BIRCH algorithm
    """


class HierarchicalCluster:
    """
    SparkPinkMST
    """


class HiddenMarkov:
    """
    SparkPinkMST
    """
