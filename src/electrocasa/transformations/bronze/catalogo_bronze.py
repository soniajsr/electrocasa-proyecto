from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.schemas.bronze.catalogo import catalogo_bronze_schema

landing_base = spark.conf.get("electrocasa.landing_path")

@dp.table(
    name="bronze.catalogo",
    comment="Ingesta raw incremental con Auto Loader para catalogo",
    partition_cols=["_ingest_date"]
)
def catalogo_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "false")
        .schema(catalogo_bronze_schema)
        .load(f"{landing_base}/catalogo/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )
