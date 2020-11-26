from pyspark.ml import Pipeline
from pyspark.sql.dataframe import DataFrame
from ophelia.ophelib.ml import feature_engine


class MLMiner:

    ophelia_feature = feature_engine.FeatureEngine()

    @staticmethod
    def build_string_index(df, col_name, indexer_type) -> DataFrame:
        dict_indexer = {
            "single": MLMiner.ophelia_feature.single_string_indexer,
            "multi": MLMiner.ophelia_feature.multi_string_indexer
        }
        pipe_ml = Pipeline(dict_indexer[indexer_type](col_name))
        fit_model = pipe_ml.fit(dataset=df)
        return fit_model.transform(df)

    @staticmethod
    def build_one_hot_encoder(df: DataFrame, col_name: list) -> DataFrame:
        encoder = MLMiner.ophelia_feature.ohe_estimator(col_name)
        encode_vector = encoder.fit(dataset=df)
        return encode_vector.transform(df)

    @staticmethod
    def build_vector_assembler(df, col_name):
        vec_assembler = MLMiner.ophelia_feature.vector_assembler(col_name)
        return vec_assembler.transform(df)
