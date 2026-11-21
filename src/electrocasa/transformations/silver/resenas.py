from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.utils.utils import parsear_fecha_flexible

@dp.table(name="silver.resenas", comment="Reseñas limpias y deduplicadas")
@dp.expect_or_drop("calificacion_rango", "calificacion BETWEEN 1 AND 5")
def resenas_silver():
    return (
        spark.readStream.table("bronze.resenas")
        .withColumn("fecha_resena", parsear_fecha_flexible(F.col("fecha_resena")))
        .withColumn("calificacion", F.col("calificacion").cast("int"))
        .dropDuplicates(["resena_id"])
    )
