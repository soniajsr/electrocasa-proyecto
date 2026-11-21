from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.despachos_courier",
    comment="P6: Tiempo promedio de entrega (horas) y porcentaje de envíos devueltos por courier"
)
def despachos_courier():
    eventos = spark.read.table("silver.tracking_envios")

    por_envio = (
        eventos.groupBy("tracking_id", "courier")
        .agg(
            F.min(F.when(F.col("estado_entrega") == "pendiente", F.col("fecha_modificacion"))).alias("ts_pendiente"),
            F.min(F.when(F.col("estado_entrega") == "entregado", F.col("fecha_modificacion"))).alias("ts_entregado"),
            F.max(F.when(F.col("estado_entrega") == "entregado", 1).otherwise(0)).alias("es_entregado"),
            F.max(F.when(F.col("estado_entrega") == "devuelto", 1).otherwise(0)).alias("es_devuelto")
        )
        .withColumn(
            "horas_entrega",
            F.when(
                (F.col("es_entregado") == 1) & F.col("ts_pendiente").isNotNull() & F.col("ts_entregado").isNotNull(),
                (F.unix_timestamp("ts_entregado") - F.unix_timestamp("ts_pendiente")) / 3600.0
            )
        )
    )

    return (
        por_envio.filter("es_entregado = 1 OR es_devuelto = 1")
        .groupBy("courier")
        .agg(
            F.round(F.avg("horas_entrega"), 2).alias("tiempo_promedio_entrega_horas"),
            F.count("*").alias("envios_estado_final"),
            F.sum("es_devuelto").alias("envios_devueltos"),
            F.round((F.sum("es_devuelto") / F.count("*")) * 100, 2).alias("porcentaje_devueltos")
        )
    )
