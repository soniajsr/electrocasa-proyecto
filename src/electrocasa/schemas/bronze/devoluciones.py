from pyspark.sql.types import StructType, StructField, StringType

devoluciones_bronze_schema = StructType([
    StructField("devolucion_id", StringType(), True),
    StructField("venta_id", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("motivo", StringType(), True),
    StructField("monto_reembolso", StringType(), True),
    StructField("fecha_devolucion", StringType(), True),
])