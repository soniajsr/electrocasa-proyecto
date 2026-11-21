from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.despachos_courier",
    comment="P6: Tiempo promedio de entrega (horas) y porcentaje de envíos devueltos por courier"
)
def despachos_courier():
    # Implementar agregación analítica sobre las tablas limpias de silver
    return spark.sql("SELECT current_timestamp() AS fecha_generacion")
