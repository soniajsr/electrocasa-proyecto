from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="gold.resenas_categoria",
    comment="P5: Porcentaje de reseñas negativas por categoría vigente y mes"
)
def resenas_categoria():
    w = Window.partitionBy("producto_id").orderBy(F.desc("fecha_snapshot"))
    catalogo_vigente = (
        spark.read.table("silver.catalogo")
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select("producto_id", "categoria")
    )

    resenas = (
        spark.read.table("silver.resenas")
        .filter("fecha_resena BETWEEN '2026-03-01' AND '2026-10-31'")
        .filter("calificacion BETWEEN 1 AND 5")
    )

    return (
        resenas.join(catalogo_vigente, on="producto_id", how="inner")
        .withColumn("mes", F.date_format("fecha_resena", "yyyy-MM"))
        .groupBy("categoria", "mes")
        .agg(
            F.count("*").alias("total_resenas"),
            F.sum(F.when(F.col("calificacion").isin(1, 2), 1).otherwise(0)).alias("resenas_negativas")
        )
        .withColumn(
            "porcentaje_negativas",
            F.round((F.col("resenas_negativas") / F.col("total_resenas")) * 100, 2)
        )
    )
