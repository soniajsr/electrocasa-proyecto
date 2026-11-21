from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.ventas_vendedor",
    comment="P4: Ventas de tienda física por vendedor, sucursal histórica SCD2 y mes"
)
def ventas_vendedor():
    ventas_fisicas = (
        spark.read.table("silver.ventas")
        .filter("fecha_venta BETWEEN '2026-03-01' AND '2026-10-31'")
        .filter(F.lower(F.trim(F.col("canal"))).isin("tienda_fisica", "tienda física", "fisica"))
    )

    empleados = spark.read.table("silver.empleados")

    cruce = ventas_fisicas.alias("v").join(
        empleados.alias("e"),
        (F.col("v.vendedor_id") == F.col("e.id_empleado")) &
        (F.col("e.__START_AT.fecha_evento_dt") <= F.col("v.fecha_venta")) &
        (F.col("e.__END_AT.fecha_evento_dt").isNull() | (F.col("e.__END_AT.fecha_evento_dt") > F.col("v.fecha_venta"))),
        how="left"
    )

    return (
        cruce.withColumn("sucursal_historica", F.coalesce(F.col("e.sucursal_id"), F.lit("SIN_ASIGNAR")))
        .withColumn("mes", F.date_format("v.fecha_venta", "yyyy-MM"))
        .groupBy("v.vendedor_id", "sucursal_historica", "mes")
        .agg(
            F.sum("v.monto_total").alias("monto_ventas"),
            F.countDistinct("v.venta_id").alias("cantidad_ventas")
        )
    )