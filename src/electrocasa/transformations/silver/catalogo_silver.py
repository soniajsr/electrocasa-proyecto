from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.utils.utils import limpiar_decimal, parsear_fecha_flexible

@dp.table(name="silver.catalogo", comment="Catálogo de productos limpio y deduplicado")
@dp.expect_or_drop("precio_valido", "precio_lista > 0")
def catalogo_silver():
    return (
        spark.readStream.table("bronze.catalogo")
        .withColumn("precio_lista", limpiar_decimal(F.col("precio_lista")))
        .withColumn("fecha_snapshot", parsear_fecha_flexible(F.col("fecha_snapshot")))
        .withColumn("activo", F.col("activo").cast("boolean"))
        .dropDuplicates(["producto_id", "fecha_snapshot"])
    )
