from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.schemas.bronze.empleados import empleados_bronze_schema

landing_base = spark.conf.get("electrocasa.landing_path")

@dp.table(
    name="bronze.empleados",
    comment="Ingesta raw incremental con Auto Loader para empleados",
    partition_cols=["_ingest_date"]
)
def empleados_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(empleados_bronze_schema)
        .load(f"{landing_base}/empleados/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )
