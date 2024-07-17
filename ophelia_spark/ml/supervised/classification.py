import numpy as np
from pyspark.ml import Transformer
from pyspark.ml.classification import (
    BinaryLogisticRegressionTrainingSummary,
    LogisticRegression,
    LogisticRegressionModel,
    RandomForestClassificationModel,
)
from pyspark.ml.linalg import DenseMatrix, DenseVector
from pyspark.sql import DataFrame


class LogitRegression:

    @staticmethod
    def __build_model_parameters(**kargs) -> dict:
        features_col = str(kargs.get("featuresCol"))
        label_col = str(kargs.get("labelCol"))
        max_iter = int(kargs.get("maxIter"))
        prediction_col = str(kargs.get("predictionCol"))
        reg_param = float(kargs.get("regParam"))
        elastic_net_param = float(kargs.get("elasticNetParam"))
        tolerance = float(kargs.get("tol"))
        fit_intercept = bool(kargs.get("fitIntercept"))
        threshold = float(kargs.get("threshold"))
        thresholds = kargs.get("thresholds")
        probability_col = str(kargs.get("probabilityCol"))
        raw_prediction_col = str(kargs.get("rawPredictionCol"))
        standardization = bool(kargs.get("standardization"))
        weight_col = kargs.get("weightCol")
        aggregation_depth = int(kargs.get("aggregationDepth"))
        family = str(kargs.get("family"))
        lower_bounds_coefficients = kargs.get("lowerBoundsOnCoefficients")
        upper_bounds_coefficients = kargs.get("upperBoundsOnCoefficients")
        lower_bounds_intercepts = kargs.get("lowerBoundsOnIntercepts")
        upper_bounds_intercepts = kargs.get("upperBoundsOnIntercepts")
        conf = {
            "features_col": "features" if features_col is None else features_col,
            "label_col": "label" if label_col is None else label_col,
            "max_iter": 10 if max_iter is None else max_iter,
            "prediction_col": (
                "prediction" if prediction_col is None else prediction_col
            ),
            "reg_param": 0.0 if reg_param is None else reg_param,
            "elastic_net_param": (
                0.0 if elastic_net_param is None else elastic_net_param
            ),
            "tolerance": 1e-6 if tolerance is None else tolerance,
            "fit_intercept": True if fit_intercept is None else fit_intercept,
            "threshold": 0.5 if threshold is None else threshold,
            "thresholds": None if thresholds is None else thresholds,
            "probability_col": (
                "probability" if probability_col is None else probability_col
            ),
            "raw_prediction_col": (
                "rawPrediction" if raw_prediction_col is None else raw_prediction_col
            ),
            "standardization": True if standardization is None else standardization,
            "weight_col": None if weight_col is None else weight_col,
            "aggregation_depth": 2 if aggregation_depth is None else aggregation_depth,
            "family": "auto" if family is None else family,
            "lower_bounds_coefficients": (
                None if lower_bounds_coefficients is None else lower_bounds_coefficients
            ),
            "upper_bounds_coefficients": (
                None if upper_bounds_coefficients is None else upper_bounds_coefficients
            ),
            "lower_bounds_intercepts": (
                None if lower_bounds_intercepts is None else lower_bounds_intercepts
            ),
            "upper_bounds_intercepts": (
                None if upper_bounds_intercepts is None else upper_bounds_intercepts
            ),
        }
        return conf

    @staticmethod
    def __lr_model_instance(config_params: dict) -> dict:
        lr = LogisticRegression(
            featuresCol=config_params["features_col"],
            labelCol=config_params["label_col"],
            maxIter=config_params["max_iter"],
            predictionCol=config_params["prediction_col"],
            regParam=config_params["reg_param"],
            elasticNetParam=config_params["elastic_net_param"],
            tol=config_params["tolerance"],
            fitIntercept=config_params["fit_intercept"],
            threshold=config_params["threshold"],
            thresholds=config_params["thresholds"],
            probabilityCol=config_params["probability_col"],
            rawPredictionCol=config_params["raw_prediction_col"],
            standardization=config_params["standardization"],
            weightCol=config_params["weight_col"],
            aggregationDepth=config_params["aggregation_depth"],
            family=config_params["family"],
            lowerBoundsOnCoefficients=config_params["lower_bounds_coefficients"],
            upperBoundsOnCoefficients=config_params["upper_bounds_coefficients"],
            lowerBoundsOnIntercepts=config_params["lower_bounds_intercepts"],
            upperBoundsOnIntercepts=config_params["upper_bounds_intercepts"],
        )
        return {"model_instance": lr, "model_params": config_params}

    @staticmethod
    def train(df: DataFrame, **kargs) -> LogisticRegressionModel:
        config_parameters = LogitRegression.__build_model_parameters(**kargs)
        lr, model_params = LogitRegression.__lr_model_instance(config_parameters)
        return lr["model_instance"].fit(df)

    @staticmethod
    def coefficient_matrix(model_train: LogisticRegressionModel) -> DenseMatrix:
        return model_train.coefficientMatrix

    @staticmethod
    def coefficient_vector(model_train: LogisticRegressionModel) -> DenseVector:
        return model_train.coefficient

    @staticmethod
    def beta(model_train: LogisticRegressionModel) -> np.ndarray:
        coefficient_vector = LogitRegression.coefficient_vector(model_train)
        return np.sort(coefficient_vector)

    @staticmethod
    def summary(
        model_train: LogisticRegressionModel,
    ) -> BinaryLogisticRegressionTrainingSummary:
        return model_train.summary

    @staticmethod
    def roc(model_train: LogisticRegressionModel) -> DataFrame:
        summary = LogitRegression.summary(model_train)
        return summary.roc

    @staticmethod
    def precision_recall(model_train: LogisticRegressionModel) -> DataFrame:
        summary = LogitRegression.summary(model_train)
        return summary.pr

    @staticmethod
    def auc_train(model_train: LogisticRegressionModel) -> DataFrame:
        summary = LogitRegression.summary(model_train)
        return summary.areaUnderROC


class RandomForestModel(Transformer):
    def __init__(
        self, num_trees=20, max_depth=5, label_col="label", features_col="features"
    ):
        super().__init__()
        self.num_trees = num_trees
        self.max_depth = max_depth
        self.label_col = label_col
        self.features_col = features_col
        self.model = RandomForestClassificationModel(
            numTrees=self.num_trees,
            maxDepth=self.max_depth,
            labelCol=self.label_col,
            featuresCol=self.features_col,
        )

    def _transform(self, dataset):
        return self.model.transform(dataset)
