from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.utils.utils import limpiar_decimal, parsear_fecha_flexible

@dp.table(name="silver.ventas", comment="Ventas limpias y deduplicadas")
@dp.expect_or_drop("monto_positivo", "monto_total > 0")
def ventas_silver():
    return (
        spark.readStream.table("bronze.ventas")
        .withColumn("fecha_venta", parsear_fecha_flexible(F.col("fecha_venta")))
        .withColumn("monto_total", limpiar_decimal(F.col("monto_total")))
        .withColumn("cantidad", F.col("cantidad").cast("int"))
        .dropDuplicates(["venta_id"])
    )
