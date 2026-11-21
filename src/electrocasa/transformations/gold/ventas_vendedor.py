from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.ventas_vendedor",
    comment="P4: Ventas de tienda física por vendedor, sucursal histórica SCD2 y mes"
)
def ventas_vendedor():
    # Implementar agregación analítica sobre las tablas limpias de silver
    return spark.sql("SELECT current_timestamp() AS fecha_generacion")
