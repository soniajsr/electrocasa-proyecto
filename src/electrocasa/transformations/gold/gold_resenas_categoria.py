from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.gold_resenas_categoria",
    comment="P5: Porcentaje de reseñas negativas por categoría vigente y mes"
)
def gold_resenas_categoria():
    # Implementar agregación analítica sobre las tablas limpias de silver
    return spark.sql("SELECT current_timestamp() AS fecha_generacion")
