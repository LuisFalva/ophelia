from com.ophelia import DataFrame
from com.ophelia.ml import OpheliaFeature

ophelia_feature = OpheliaFeature.OpheliaFeature()


class OpheliaBuilder:

    @staticmethod
    def build_string_index(df, col_name, indexer_type) -> DataFrame:
        dict_indexer = {
            "single": ophelia_feature.single_string_indexer,
            "multi": ophelia_feature.multi_string_indexer
        }
        pipe_ml = ophelia_feature.pipe(dict_indexer[indexer_type](col_name))
        fit_model = ophelia_feature.fit(df, pipe_ml)
        return ophelia_feature.transform(fit_model, df)

    @staticmethod
    def build_one_hot_encoder(df, col_name) -> DataFrame:
        encoder = ophelia_feature.ohe_estimator(col_name)
        encode_vector = ophelia_feature.fit(df, encoder)
        return ophelia_feature.transform(encode_vector, df)

    @staticmethod
    def build_vector_assembler(df, col_name):
        vec_assembler = ophelia_feature.vector_assembler(col_name)
        return ophelia_feature.transform(df, vec_assembler)
