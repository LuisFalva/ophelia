class Ophelia:

    def __init__(self, app_name=None, no_mask=False, meta_info=False):
        from ._info import OpheliaInfo
        from .write.spark_write import Write
        from .read.spark_read import Read
        from .session.spark import OpheliaSpark
        from ._wrapper import (
            NullDebugWrapper,
            CorrMatWrapper,
            ShapeWrapper,
            RollingWrapper,
            DynamicSamplingWrapper,
            SelectsWrapper,
            MapItemWrapper,
            ReshapeWrapper,
            PctChangeWrapper,
            CrossTabularWrapper,
            JoinsWrapper,
            DaskSparkWrapper,
        )
        from .ml.feature_miner import (
            BuildStandardScaler,
            BuildVectorAssembler,
            BuildStringIndex,
            BuildOneHotEncoder,
            NumpyToVector,
            SparkToNumpy
        )
        from .ml.unsupervised.feature import (
            PCAnalysis,
            SingularVD
        )
        from .ml.sampling.synthetic_sample import SyntheticSample
        transformers = {
            'spark_write': Write,
            'spark_read': Read,
            'ophelia_spark': OpheliaSpark,
            # Spark wrapper functions class
            'null_debug': NullDebugWrapper,
            'correlation': CorrMatWrapper,
            'shape': ShapeWrapper,
            'rolling': RollingWrapper,
            'sampling': DynamicSamplingWrapper,
            'selects': SelectsWrapper,
            'map_item': MapItemWrapper,
            'reshape': ReshapeWrapper,
            'pct_change': PctChangeWrapper,
            'crosstab': CrossTabularWrapper,
            'joins': JoinsWrapper,
            'dask_spark': DaskSparkWrapper,
            # Feature miner class
            'standard_scaler': BuildStandardScaler,
            'vector_assembler': BuildVectorAssembler,
            'string_index': BuildStringIndex,
            'one_hot_encode': BuildOneHotEncoder,
            # Spark ML pipelines
            'numpy_to_vector': NumpyToVector,
            'spark_to_numpy': SparkToNumpy,
            'pca_analysis': PCAnalysis,
            'singular_vd': SingularVD,
            'smote_sample': SyntheticSample
        }
        OpheliaInfo(transformers).print_info(no_mask, meta_info)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.SC = self.Spark.build_spark_context()
