from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.gold_dotacion_mensual",
    comment="P3: Empleados activos por sucursal al último día de cada mes (marzo-octubre 2026)"
)
def gold_dotacion_mensual():
    # Implementar agregación analítica sobre las tablas limpias de silver
    return spark.sql("SELECT current_timestamp() AS fecha_generacion")
