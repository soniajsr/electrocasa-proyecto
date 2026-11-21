from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.utils.utils import limpiar_decimal, parsear_fecha_flexible

@dp.view(name="v_devoluciones_enriquecidas")
def v_devoluciones_enriquecidas():
    dev = (
        spark.readStream.table("bronze.devoluciones")
        .withColumn("fecha_devolucion", parsear_fecha_flexible(F.col("fecha_devolucion")))
        .withColumn("monto_reembolso", limpiar_decimal(F.col("monto_reembolso")))
        .dropDuplicates(["devolucion_id"])
    )
    ventas = spark.read.table("silver.ventas").select("venta_id", F.col("monto_total").alias("monto_venta_origen"))
    return dev.join(ventas, on="venta_id", how="left")

@dp.table(name="silver.devoluciones", comment="Devoluciones validadas contra venta de origen")
@dp.expect_or_drop("reembolso_valido", "monto_reembolso >= 0 AND (monto_venta_origen IS NULL OR monto_reembolso <= monto_venta_origen)")
def devoluciones_silver():
    return spark.readStream.table("v_devoluciones_enriquecidas").drop("monto_venta_origen")
