from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, DoubleType, IntegerType
from com.ophelia.OpheliaVendata import OpheliaVendata
from com.ophelia import BuildLoad


class OpheliaRiskClassifier:
    # TODO: se necesita más código para tener el módulo completo
    ophelia = OpheliaVendata("Ophelia Risk Classifier")
    spark = ophelia.ophelia_session
    BuildLoad()
    path = "data/master/ophelia/data/OpheliaData/risk_classification/"
    random_sample_df = spark.read.parquet(path)
    schema_tree = ArrayType(StructType([
        StructField("weight", DoubleType(), True),
        StructField("risk_label", StringType(), True),
        StructField("vote", IntegerType(), True)
    ]))
    tree_udf = udf(ophelia.ophelia_class.tree_generator, schema_tree)
    classification_udf = udf(ophelia.ophelia_class.run_classification_risk, StringType())
    result_df = random_sample_df.select("*", tree_udf(col("struct")).alias("tree")).cache()
    col_list = ["age", "job", "marital", "education", "gender", "child", "saving", "insight", "backup"]
    final = result_df.select(col_list, classification_udf(col("tree")).alias("risk_label"))
    ophelia.ophelia_spark.clear_cache()
