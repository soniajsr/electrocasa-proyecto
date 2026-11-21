from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.ventas_sucursal_mes",
    comment="P1: Ventas netas, devoluciones, cantidad de ventas y ticket promedio por sucursal y mes"
)
def ventas_sucursal_mes():
    # Implementar agregación analítica sobre las tablas limpias de silver
    return spark.sql("SELECT current_timestamp() AS fecha_generacion")
