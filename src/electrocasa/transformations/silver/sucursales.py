from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(name="silver.sucursales", comment="Maestro limpio de sucursales")
def sucursales_silver():
    return (
        spark.readStream.table("bronze.sucursales")
        .withColumn("ciudad", F.initcap(F.trim(F.col("ciudad"))))
        .withColumn("region", F.initcap(F.trim(F.col("region"))))
        .dropDuplicates(["sucursal_id"])
    )
