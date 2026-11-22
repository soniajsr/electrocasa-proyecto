from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.view(name="v_devoluciones_limpias")
def v_devoluciones_limpias():
    dev = spark.readStream.table("bronze.devoluciones")
    ventas = spark.read.table("silver.ventas").select(
        F.col("venta_id"),
        F.col("monto_total").alias("monto_venta_origen")
    )

    return (
        dev.withColumn("monto_reembolso", F.regexp_replace(F.col("monto_reembolso"), "[^0-9.,-]", ""))
        .withColumn("monto_reembolso", F.regexp_replace(F.col("monto_reembolso"), ",", ".").cast("decimal(12,2)"))
        .withColumn("fecha_devolucion", F.coalesce(
            F.to_date("fecha_devolucion", "yyyy-MM-dd"),
            F.to_date("fecha_devolucion", "dd/MM/yyyy")
        ))
        .dropDuplicates(["devolucion_id"])
        .join(ventas, on="venta_id", how="left")
    )

@dp.table(
    name="silver.devoluciones",
    comment="Devoluciones validadas contra el monto de la venta original"
)
@dp.expect_or_drop(
    "reembolso_valido",
    "monto_reembolso >= 0 AND (monto_venta_origen IS NULL OR monto_reembolso <= monto_venta_origen)"
)
def silver_devoluciones():
    return dp.read_stream("v_devoluciones_limpias").drop("monto_venta_origen")
