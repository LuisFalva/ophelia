class Ophelia:

    def __init__(self, app_name=None, no_mask=False):
        from ._info import OpheliaInfo
        from .write.spark_write import Write
        from .read.spark_read import Read
        from .session.spark import OpheliaSpark
        from .functions import (
            SelectWrapper,
            RollingWrapper,
            ReshapeWrapper,
            DaskSparkWrapper,
            DynamicSamplingWrapper,
            CorrMatWrapper,
            CrossTabularWrapper,
            PctChangeWrapper
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
            'spark_read': Read,
            'spark_write': Write,
            'selects': SelectWrapper,
            'rolling': RollingWrapper,
            'reshape': ReshapeWrapper,
            'dask_spark': DaskSparkWrapper,
            'sampling': DynamicSamplingWrapper,
            'correlation': CorrMatWrapper,
            'crosstab': CrossTabularWrapper,
            'pct_change': PctChangeWrapper,
            'standard_scaler': BuildStandardScaler,
            'vector_assembler': BuildVectorAssembler,
            'string_index': BuildStringIndex,
            'one_hot_encode': BuildOneHotEncoder,
            'numpy_to_vector': NumpyToVector,
            'spark_to_numpy': SparkToNumpy,
            'pca_analysis': PCAnalysis,
            'singular_vd': SingularVD,
            'smote_sample': SyntheticSample
        }
        OpheliaInfo(transformers).print_info(no_mask)
        self.Spark = OpheliaSpark()
        self.SparkSession = self.Spark.build_spark_session(app_name)
        self.SC = self.Spark.build_spark_context()
