from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.schemas.bronze.sucursales import sucursales_bronze_schema

landing_base = spark.conf.get("electrocasa.landing_path")

@dp.table(
    name="bronze.sucursales",
    comment="Ingesta raw incremental con Auto Loader para sucursales",
    partition_cols=["_ingest_date"]
)
def sucursales_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(sucursales_bronze_schema)
        .load(f"{landing_base}/sucursales/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )
