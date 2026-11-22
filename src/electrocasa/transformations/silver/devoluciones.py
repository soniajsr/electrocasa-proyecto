from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.view(name="v_devoluciones_limpias")
def v_devoluciones_limpias():
    dev = spark.readStream.table("bronze.devoluciones")
    ventas = spark.read.table("silver.ventas").select(
        F.col("venta_id"),
        F.col("monto_total").alias("monto_venta_origen")
    )

    df_joined = (
        dev.withColumn("monto_reembolso", F.regexp_replace(F.col("monto_reembolso"), "[^0-9.,-]", ""))
        .withColumn("monto_reembolso", F.regexp_replace(F.col("monto_reembolso"), ",", ".").cast("decimal(12,2)"))
        .withColumn("fecha_devolucion", F.coalesce(
            F.try_to_date("fecha_devolucion", "yyyy-MM-dd"),
            F.try_to_date("fecha_devolucion", "dd/MM/yyyy")
        ))
        .dropDuplicates(["devolucion_id"])
        .join(ventas, on="venta_id", how="left")
    )

    # Calculamos la regla booleana explícitamente en el DataFrame
    return df_joined.withColumn(
        "_reembolso_valido",
        (F.col("monto_reembolso") >= 0) &
        (F.col("monto_venta_origen").isNull() | (F.col("monto_reembolso") <= F.col("monto_venta_origen")))
    ).drop("monto_venta_origen")


@dp.table(
    name="silver.devoluciones",
    comment="Devoluciones validadas contra el monto de la venta original"
)
@dp.expect_or_drop("reembolso_valido", "_reembolso_valido = true")
def silver_devoluciones():
    return dp.read_stream("v_devoluciones_limpias").drop("_reembolso_valido")