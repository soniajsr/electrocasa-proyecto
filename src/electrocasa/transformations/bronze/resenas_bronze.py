from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.schemas.bronze.resenas import resenas_bronze_schema

landing_base = spark.conf.get("electrocasa.landing_path")

@dp.table(
    name="bronze.resenas",
    comment="Ingesta raw incremental con Auto Loader para resenas",
    partition_cols=["_ingest_date"]
)
def resenas_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "false")
        .schema(resenas_bronze_schema)
        .load(f"{landing_base}/resenas/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )
