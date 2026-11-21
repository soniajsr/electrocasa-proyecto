from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="gold.dotacion_mensual",
    comment="P3: Empleados activos por sucursal al último día de cada mes (marzo-octubre 2026)"
)
def dotacion_mensual():
    cierres_mes = spark.sql("""
        SELECT explode(sequence(DATE'2026-03-31', DATE'2026-10-31', INTERVAL 1 MONTH)) AS fecha_cierre
    """).withColumn("fecha_cierre", F.last_day("fecha_cierre"))

    empleados = spark.read.table("silver.empleados")

    return (
        cierres_mes.join(
            empleados,
            (F.col("__START_AT.fecha_evento_dt") <= F.col("fecha_cierre")) &
            (F.col("__END_AT.fecha_evento_dt").isNull() | (F.col("__END_AT.fecha_evento_dt") > F.col("fecha_cierre"))),
            how="inner"
        )
        .withColumn("mes", F.date_format("fecha_cierre", "yyyy-MM"))
        .groupBy("sucursal_id", "mes", "fecha_cierre")
        .agg(F.countDistinct("id_empleado").alias("empleados_activos"))
    )