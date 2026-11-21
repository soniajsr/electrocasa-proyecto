from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.gold_top_productos",
    comment="P2: Top 10 productos más vendidos y Top 10 con mayor tasa de devolución"
)
def gold_top_productos():
    # Implementar agregación analítica sobre las tablas limpias de silver
    return spark.sql("SELECT current_timestamp() AS fecha_generacion")
