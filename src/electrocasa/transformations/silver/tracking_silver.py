from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(name="silver.tracking_envios", comment="Eventos de despacho estandarizados desde bronze.tracking_envios")
@dp.expect_or_drop("estado_valido", "estado_entrega IN ('pendiente', 'en_transito', 'entregado', 'devuelto')")
def tracking_silver():
    return (
        spark.readStream.table("bronze.tracking_envios")
        .withColumn("estado_entrega", F.lower(F.trim(F.col("estado_entrega"))))
        .withColumn("courier", F.upper(F.trim(F.col("courier"))))
        .dropDuplicates(["envio_id", "estado_entrega", "fecha_modificacion"])
    )
