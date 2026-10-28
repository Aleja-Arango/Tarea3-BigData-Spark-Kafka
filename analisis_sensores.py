
from pyspark.sql.functions import to_date, count

# Analisis de temperatura y humedad por dia
print("\n=== ANALISIS DIARIO ===")

analisis_diario = sensores_limpios.withColumn(
    "dia", to_date(col("fecha"))
).groupBy("dia").agg(
    count("*").alias("total_mediciones"),
    round(avg("temperatura"), 2).alias("temperatura_promedio"),
    round(avg("humedad"), 2).alias("humedad_promedio")
).orderBy("dia")

analisis_diario.show(30, truncate=False)
