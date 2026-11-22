from pyspark import pipelines as dp
import pyspark.sql.functions as F
from electrocasa.utils.utils import limpiar_decimal, parsear_fecha_flexible

catalogo = spark.conf.get("electrocasa.catalogo")

@dp.view(name="v_empleados_eventos_limpios")
@dp.expect_or_drop("llave_y_fecha_validas", "id_empleado IS NOT NULL AND fecha_evento IS NOT NULL")
def v_empleados_eventos_limpios():
    return (
        spark.readStream.table("bronze.empleados")
        .withColumn("fecha_evento_dt", parsear_fecha_flexible(F.col("fecha_evento")))
        .withColumn("salario", limpiar_decimal(F.col("salario")).cast("string"))
        .withColumn("orden_secuencia", F.struct(F.col("fecha_evento_dt"), F.col("secuencia_evento")))
    )

dp.create_streaming_table(
    name="silver.empleados",
    comment="Historia SCD Type 2 de empleados con Column Masking en dni y salario",
    schema=f"""
        id_empleado STRING,
        dni STRING MASK {catalogo}.silver.mask_dato_sensible,
        nombre STRING,
        email STRING,
        sucursal_id STRING,
        cargo STRING,
        salario STRING MASK {catalogo}.silver.mask_dato_sensible,
        tipo_evento STRING,
        fecha_evento_dt DATE,
        __START_AT STRUCT<fecha_evento_dt: DATE, secuencia_evento: STRING>,
        __END_AT STRUCT<fecha_evento_dt: DATE, secuencia_evento: STRING>
    """
)

dp.create_auto_cdc_flow(
    target="silver.empleados",
    source="v_empleados_eventos_limpios",
    keys=["id_empleado"],
    sequence_by=F.col("orden_secuencia"),
    apply_as_deletes=F.expr("UPPER(tipo_evento) = 'CESE'"),
    except_column_list=["fecha_evento", "secuencia_evento", "orden_secuencia", "_ingest_timestamp", "_ingest_date", "_source_file"],
    stored_as_scd_type=2
)