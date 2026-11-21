from pyspark.sql.types import StructType, StructField, StringType

sucursales_bronze_schema = StructType([
    StructField("sucursal_id", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("distrito", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("region", StringType(), True),
])
