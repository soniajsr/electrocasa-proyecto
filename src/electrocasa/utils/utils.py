from pyspark.sql import Column
import pyspark.sql.functions as F

def limpiar_decimal(col_expr: Column) -> Column:
    """Normaliza montos con símbolos de moneda, separadores de miles y decimales a DECIMAL(12,2)."""
    limpio = F.regexp_replace(F.trim(col_expr.cast("string")), r"[^0-9.,-]", "")
    normalizado = F.when(
        limpio.contains(",") & limpio.contains("."),
        F.regexp_replace(limpio, ",", "")
    ).when(
        limpio.contains(","),
        F.regexp_replace(limpio, ",", ".")
    ).otherwise(limpio)
    return normalizado.cast("decimal(12,2)")

def parsear_fecha_flexible(col_expr: Column) -> Column:
    """Parsea múltiples formatos de fecha hacia DATE."""
    return F.coalesce(
        F.try_to_date(col_expr, "yyyy-MM-dd"),
        F.try_to_date(col_expr, "dd/MM/yyyy"),
        F.try_to_date(col_expr, "yyyy/MM/dd"),
        F.try_to_date(col_expr, "dd-MM-yyyy")
    )