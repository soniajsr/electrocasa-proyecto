from pyspark.sql.types import StructType, StructField, StringType

empleados_bronze_schema = StructType([
    StructField("id_empleado", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("dni", StringType(), True),
    StructField("email", StringType(), True),
    StructField("salario", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("fecha_evento", StringType(), True),
    StructField("secuencia_evento", StringType(), True),
])