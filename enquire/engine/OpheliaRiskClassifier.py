from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, DoubleType, IntegerType
from ophelia.ophelib.OpheliaMain import Ophelia
from enquire.engine.OpheliaClassifier import OpheliaClassifier


class OpheliaRiskClassifier:
    # TODO: se necesita más código para tener el módulo completo
    ophelia = Ophelia("Ophelia Risk Classifier")
    spark = ophelia.SparkSession
    supervised = OpheliaClassifier()
    path = "data/master/ophelia/data/OpheliaData/risk_classification/"
    random_sample_df = spark.read.parquet(path)
    schema_tree = ArrayType(StructType([
        StructField("weight", DoubleType(), True),
        StructField("risk_label", StringType(), True),
        StructField("vote", IntegerType(), True)
    ]))
    tree_udf = udf(supervised.tree_generator, schema_tree)
    classification_udf = udf(supervised.run_classification_risk, StringType())
    result_df = random_sample_df.select("*", tree_udf(col("struct")).alias("tree")).cache()
    col_list = ["age", "job", "marital", "education", "gender", "child", "saving", "insight", "backup"]
    final = result_df.select(col_list, classification_udf(col("tree")).alias("risk_label"))
    ophelia.Spark.clear_cache()
