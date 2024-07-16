from pyspark.ml import Transformer
from pyspark.ml.clustering import BisectingKMeans


class KMeansCluster(Transformer):
    """
    traditional spark clustering
    """


class FuzzyClusterMeans(Transformer):
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
        super().__init__()
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
        fuzzy_kmeans = FuzzyKMeans(
            k=self.k, m=self.m, maxIter=self.max_iter, tol=self.tol
        )
        model = fuzzy_kmeans.fit(self.data)
        return model

    def _transform(self, model, data):
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


class BKM(Transformer):
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

    def __init__(self, k=2, max_iter=20, seed=None, features_col="features"):
        super().__init__()
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
        bkm = BisectingKMeans(
            k=self.k,
            maxIter=self.max_iter,
            seed=self.seed,
            featuresCol=self.features_col,
        )
        self.model = bkm.fit(data)
        return self.model

    def _transform(self, data):
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


class GaussianM(Transformer):
    """
    traditional spark clustering
    """


class LatentDirichlet(Transformer):
    """
    traditional spark clustering
    """


class ClusterFeatureTree(Transformer):
    """
    BIRCH algorithm
    """


class HierarchicalCluster(Transformer):
    """
    SparkPinkMST
    """


class HiddenMarkov(Transformer):
    def __init__(
        self,
        num_states,
        num_observations,
        transition_probabilities,
        observation_probabilities,
        initial_probabilities,
    ):
        super().__init__()
        self.num_states = num_states
        self.num_observations = num_observations
        self.transition_probabilities = transition_probabilities
        self.observation_probabilities = observation_probabilities
        self.initial_probabilities = initial_probabilities

    def forward(self, observations):
        T = len(observations)
        alpha = [[0.0 for _ in range(self.num_states)] for _ in range(T)]
        alpha[0] = [
            self.initial_probabilities[i]
            * self.observation_probabilities[i][observations[0]]
            for i in range(self.num_states)
        ]
        for t in range(1, T):
            for j in range(self.num_states):
                alpha[t][j] = (
                    sum(
                        [
                            alpha[t - 1][i] * self.transition_probabilities[i][j]
                            for i in range(self.num_states)
                        ]
                    )
                    * self.observation_probabilities[j][observations[t]]
                )
        return alpha

    def backward(self, observations):
        T = len(observations)
        beta = [[0.0 for _ in range(self.num_states)] for _ in range(T)]
        beta[T - 1] = [1.0 for _ in range(self.num_states)]
        for t in range(T - 2, -1, -1):
            for i in range(self.num_states):
                beta[t][i] = sum(
                    [
                        self.transition_probabilities[i][j]
                        * self.observation_probabilities[j][observations[t + 1]]
                        * beta[t + 1][j]
                        for j in range(self.num_states)
                    ]
                )
        return beta

    def viterbi(self, observations):
        T = len(observations)
        delta = [[0.0 for _ in range(self.num_states)] for _ in range(T)]
        psi = [[0 for _ in range(self.num_states)] for _ in range(T)]
        delta[0] = [
            self.initial_probabilities[i]
            * self.observation_probabilities[i][observations[0]]
            for i in range(self.num_states)
        ]
        for t in range(1, T):
            for j in range(self.num_states):
                delta[t][j] = max(
                    [
                        delta[t - 1][i]
                        * self.transition_probabilities[i][j]
                        * self.observation_probabilities[j][observations[t]]
                        for i in range(self.num_states)
                    ]
                )
                psi[t][j] = max(
                    [
                        (delta[t - 1][i] * self.transition_probabilities[i][j], i)
                        for i in range(self.num_states)
                    ]
                )[1]
        path = [max([(delta[T - 1][i], i) for i in range(self.num_states)])[1]]
        for t in range(T - 1, 0, -1):
            path.insert(0, psi[t][path[0]])
        return path
