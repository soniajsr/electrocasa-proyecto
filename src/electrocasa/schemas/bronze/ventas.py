from pyspark.sql.types import StructType, StructField, StringType

ventas_bronze_schema = StructType([
    StructField("venta_id", StringType(), True),
    StructField("fecha_venta", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("canal", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cantidad", StringType(), True),
    StructField("monto_total", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("vendedor_id", StringType(), True),
])
