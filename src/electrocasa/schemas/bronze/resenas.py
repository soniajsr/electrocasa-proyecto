from pyspark.sql.types import StructType, StructField, StringType, ArrayType

resenas_bronze_schema = StructType([
    StructField("resena_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("fecha_resena", StringType(), True),
    StructField("calificacion", StringType(), True),
    StructField("comentario", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("respuestas", ArrayType(StructType([
        StructField("autor", StringType(), True),
        StructField("texto", StringType(), True),
        StructField("fecha", StringType(), True),
    ])), True),
    StructField("metadata", StringType(), True),
])
