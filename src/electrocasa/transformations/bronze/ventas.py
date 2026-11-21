from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.schemas.bronze.ventas import ventas_bronze_schema

landing_base = spark.conf.get("electrocasa.landing_path")

@dp.table(
    name="bronze.ventas",
    comment="Ingesta raw incremental con Auto Loader para ventas",
    partition_cols=["_ingest_date"]
)
def ventas_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(ventas_bronze_schema)
        .load(f"{landing_base}/ventas/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )
