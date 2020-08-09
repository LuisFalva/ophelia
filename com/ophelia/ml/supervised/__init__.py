from pyspark.ml.classification import LinearSVC, LinearSVCModel, LogisticRegression, LogisticRegressionModel, \
    LogisticRegressionSummary, BinaryLogisticRegressionTrainingSummary, LogisticRegressionTrainingSummary, \
    BinaryLogisticRegressionSummary, DecisionTreeClassifier, DecisionTreeClassificationModel, GBTClassifier, \
    GBTClassificationModel, RandomForestClassifier, RandomForestClassificationModel, NaiveBayes, NaiveBayesModel, \
    MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel, OneVsRest, OneVsRestModel
from pyspark.ml.regression import AFTSurvivalRegression, AFTSurvivalRegressionModel, DecisionTreeRegressor, \
    DecisionTreeRegressionModel, GBTRegressor, GBTRegressionModel, GeneralizedLinearRegression, \
    GeneralizedLinearRegressionModel, GeneralizedLinearRegressionSummary, GeneralizedLinearRegressionTrainingSummary, \
    IsotonicRegression, IsotonicRegressionModel, LinearRegression, LinearRegressionModel, LinearRegressionSummary, \
    LinearRegressionTrainingSummary, RandomForestRegressor, RandomForestRegressionModel


class OpheliaClassificationObjects:

    __all__ = [LinearSVC, LinearSVCModel, LogisticRegression, LogisticRegressionModel, LogisticRegressionSummary,
               LogisticRegressionTrainingSummary, BinaryLogisticRegressionSummary,
               BinaryLogisticRegressionTrainingSummary, DecisionTreeClassifier, DecisionTreeClassificationModel,
               GBTClassifier, GBTClassificationModel, RandomForestClassifier, RandomForestClassificationModel,
               NaiveBayes, NaiveBayesModel, MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel,
               OneVsRest, OneVsRestModel]


class OpheliaRegressionObjects:

    __all__ = [AFTSurvivalRegression, AFTSurvivalRegressionModel, DecisionTreeRegressor, DecisionTreeRegressionModel,
               GBTRegressor, GBTRegressionModel, GeneralizedLinearRegression, GeneralizedLinearRegressionModel,
               GeneralizedLinearRegressionSummary, GeneralizedLinearRegressionTrainingSummary, IsotonicRegression,
               IsotonicRegressionModel, LinearRegression, LinearRegressionModel, LinearRegressionSummary,
               LinearRegressionTrainingSummary, RandomForestRegressor, RandomForestRegressionModel]
