class Ophelia:

    def __init__(self, app_name=None, no_mask=False):
        from ._info import OpheliaInfo
        from .functions import (
            CorrMatWrapper,
            CrossTabularWrapper,
            DaskSparkWrapper,
            DynamicSamplingWrapper,
            EfficientFrontierRatioCalculatorWrapper,
            JoinsWrapper,
            MapItemWrapper,
            NullDebugWrapper,
            PctChangeWrapper,
            ReshapeWrapper,
            RiskParityCalculatorWrapper,
            RollingWrapper,
            SelectsWrapper,
            ShapeWrapper,
            SharpeRatioCalculatorWrapper,
            SortinoRatioCalculatorWrapper,
        )
        from .ml.feature_miner import (
            BuildOneHotEncoder,
            BuildStandardScaler,
            BuildStringIndex,
            BuildVectorAssembler,
            NumpyToVector,
            SparkToNumpy,
        )
        from .ml.sampling.synthetic_sample import SyntheticSample
        from .ml.unsupervised.feature import PCAnalysis, SingularVD
        from .read.spark_read import Read
        from .session.spark import OpheliaSpark
        from .write.spark_write import Write

        transformers = {
            "spark_write": Write,
            "spark_read": Read,
            "ophelia_spark": OpheliaSpark,
            # Spark wrapper functions class
            "null_debug": NullDebugWrapper,
            "correlation": CorrMatWrapper,
            "shape": ShapeWrapper,
            "rolling": RollingWrapper,
            "sampling": DynamicSamplingWrapper,
            "selects": SelectsWrapper,
            "map_item": MapItemWrapper,
            "reshape": ReshapeWrapper,
            "pct_change": PctChangeWrapper,
            "crosstab": CrossTabularWrapper,
            "joins": JoinsWrapper,
            "dask_spark": DaskSparkWrapper,
            "sortino_calculator": SortinoRatioCalculatorWrapper,
            "sharpe_calculator": SharpeRatioCalculatorWrapper,
            "efficient_frontier": EfficientFrontierRatioCalculatorWrapper,
            "risk_parity": RiskParityCalculatorWrapper,
            # Feature miner class
            "standard_scaler": BuildStandardScaler,
            "vector_assembler": BuildVectorAssembler,
            "string_index": BuildStringIndex,
            "one_hot_encode": BuildOneHotEncoder,
            # Spark ML pipelines
            "numpy_to_vector": NumpyToVector,
            "spark_to_numpy": SparkToNumpy,
            "pca_analysis": PCAnalysis,
            "singular_vd": SingularVD,
            "smote_sample": SyntheticSample,
        }
        OpheliaInfo().print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.SC = self.Spark.build_spark_context()
