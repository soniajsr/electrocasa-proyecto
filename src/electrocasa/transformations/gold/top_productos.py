from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.top_productos",
    comment="P2: Top 10 productos más vendidos y Top 10 con mayor tasa de devolución"
)
def top_productos():
    ventas_prod = (
        spark.read.table("silver.ventas")
        .filter("fecha_venta BETWEEN '2026-03-01' AND '2026-10-31'")
        .groupBy("producto_id")
        .agg(
            F.sum("cantidad").alias("unidades_vendidas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )

    dev_prod = (
        spark.read.table("silver.devoluciones")
        .filter("fecha_devolucion BETWEEN '2026-03-01' AND '2026-10-31'")
        .groupBy("producto_id")
        .agg(F.countDistinct("devolucion_id").alias("cantidad_devoluciones"))
    )

    base = (
        ventas_prod.join(dev_prod, on="producto_id", how="left")
        .fillna({"cantidad_devoluciones": 0})
        .withColumn(
            "tasa_devolucion",
            F.when(F.col("cantidad_ventas") > 0, F.round(F.col("cantidad_devoluciones") / F.col("cantidad_ventas"), 4))
            .otherwise(F.lit(0))
        )
    )

    top_vendidos = (
        base.orderBy(F.desc("unidades_vendidas"))
        .limit(10)
        .withColumn("ranking_tipo", F.lit("TOP_10_MAS_VENDIDOS"))
    )

    top_devueltos = (
        base.orderBy(F.desc("tasa_devolucion"))
        .limit(10)
        .withColumn("ranking_tipo", F.lit("TOP_10_TASA_DEVOLUCION"))
    )

    return top_vendidos.unionByName(top_devueltos)