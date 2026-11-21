from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.ventas_sucursal_mes",
    comment="P1: Ventas netas, devoluciones, cantidad de ventas y ticket promedio por sucursal y mes"
)
def ventas_sucursal_mes():
    ventas = (
        spark.read.table("silver.ventas")
        .filter("fecha_venta BETWEEN '2026-03-01' AND '2026-10-31'")
        .withColumn("mes", F.date_format("fecha_venta", "yyyy-MM"))
        .groupBy("sucursal_id", "mes")
        .agg(
            F.sum("monto_total").alias("ventas_brutas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )

    devoluciones = (
        spark.read.table("silver.devoluciones")
        .filter("fecha_devolucion BETWEEN '2026-03-01' AND '2026-10-31'")
        .withColumn("mes", F.date_format("fecha_devolucion", "yyyy-MM"))
        .groupBy("sucursal_id", "mes")
        .agg(F.sum("monto_reembolso").alias("total_devoluciones"))
    )

    return (
        ventas.join(devoluciones, on=["sucursal_id", "mes"], how="full_outer")
        .fillna({"ventas_brutas": 0, "total_devoluciones": 0, "cantidad_ventas": 0})
        .withColumn("ventas_netas", F.col("ventas_brutas") - F.col("total_devoluciones"))
        .withColumn(
            "ticket_promedio",
            F.when(F.col("cantidad_ventas") > 0, F.round(F.col("ventas_brutas") / F.col("cantidad_ventas"), 2))
            .otherwise(F.lit(0))
        )
    )