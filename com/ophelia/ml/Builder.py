from pyspark.sql.dataframe import DataFrame
from com.ophelia.ml import Feature


class Builder:

    ophelia_feature = Feature.Feature()

    @staticmethod
    def build_string_index(df, col_name, indexer_type) -> DataFrame:
        dict_indexer = {
            "single": Builder.ophelia_feature.single_string_indexer,
            "multi": Builder.ophelia_feature.multi_string_indexer
        }
        pipe_ml = Builder.ophelia_feature.pipe(dict_indexer[indexer_type](col_name))
        fit_model = Builder.ophelia_feature.fit(df=df, pipe=pipe_ml)
        return Builder.ophelia_feature.transform(df=df, model=fit_model)

    @staticmethod
    def build_one_hot_encoder(df, col_name) -> DataFrame:
        encoder = Builder.ophelia_feature.ohe_estimator(col_name)
        encode_vector = Builder.ophelia_feature.fit(df, encoder)
        return Builder.ophelia_feature.transform(encode_vector, df)

    @staticmethod
    def build_vector_assembler(df, col_name):
        vec_assembler = Builder.ophelia_feature.vector_assembler(col_name)
        return Builder.ophelia_feature.transform(df, vec_assembler)
