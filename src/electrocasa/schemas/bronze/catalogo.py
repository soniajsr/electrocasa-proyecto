from pyspark.sql.types import StructType, StructField, StringType

catalogo_bronze_schema = StructType([
    StructField("producto_id", StringType(), True),
    StructField("nombre_producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("marca", StringType(), True),
    StructField("precio_lista", StringType(), True),
    StructField("activo", StringType(), True),
    StructField("fecha_snapshot", StringType(), True),
])
