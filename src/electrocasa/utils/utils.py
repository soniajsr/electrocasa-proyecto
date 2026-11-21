from pyspark.sql import Column
import pyspark.sql.functions as F

def limpiar_decimal(col_expr: Column) -> Column:
    """Normaliza montos con símbolos de moneda, puntos de miles y coma decimal a DECIMAL(12,2)."""
    limpio = F.regexp_replace(col_expr, r"[^0-9,.-]", "")
    normalizado = F.when(
        limpio.contains(","),
        F.regexp_replace(F.regexp_replace(limpio, r"\.", ""), ",", ".")
    ).otherwise(limpio)
    return normalizado.cast("decimal(12,2)")

def parsear_fecha_flexible(col_expr: Column) -> Column:
    """Parsea múltiples formatos de fecha hacia DATE."""
    return F.coalesce(
        F.to_date(col_expr, "yyyy-MM-dd"),
        F.to_date(col_expr, "dd/MM/yyyy"),
        F.to_date(col_expr, "yyyy/MM/dd"),
        F.to_date(col_expr, "dd-MM-yyyy")
    )
