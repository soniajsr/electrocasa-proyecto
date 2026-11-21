from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.utils.utils import limpiar_decimal

@dp.table(name="silver.cuarentena", comment="Registros rechazados por reglas de calidad con trazabilidad")
def cuarentena():
    rechazos_ventas = (
        spark.readStream.table("bronze.ventas")
        .withColumn("monto_num", limpiar_decimal(F.col("monto_total")))
        .filter("monto_num IS NULL OR monto_num <= 0")
        .select(
            F.lit("ventas").alias("fuente"),
            F.col("venta_id").alias("llave_registro"),
            F.lit("monto_total <= 0 o inválido").alias("motivo_rechazo"),
            F.col("_source_file").alias("archivo_origen"),
            F.current_timestamp().alias("fecha_procesamiento")
        )
    )
    return rechazos_ventas
