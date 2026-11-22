from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name="silver.devoluciones",
    comment="Devoluciones limpias y estandarizadas desde bronze.devoluciones"
)
@dp.expect_or_drop("monto_valido", "monto_reembolso >= 0")
@dp.expect_or_drop("id_no_nulo", "devolucion_id IS NOT NULL AND venta_id IS NOT NULL")
def silver_devoluciones():
    return (
        spark.readStream.table("bronze.devoluciones")
        .withColumn("monto_reembolso", F.regexp_replace(F.col("monto_reembolso"), "[^0-9.,-]", ""))
        .withColumn("monto_reembolso", F.regexp_replace(F.col("monto_reembolso"), "[^0-9.-]", "").cast("decimal(12,2)"))
        .withColumn("fecha_devolucion", F.coalesce(
            F.try_to_date("fecha_devolucion", "yyyy-MM-dd"),
            F.try_to_date("fecha_devolucion", "dd/MM/yyyy")
        ))
        .dropDuplicates(["devolucion_id"])
    )