
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg,
    from_unixtime, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, LongType
)

# Crear sesion de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Definir estructura de los mensajes
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType())
])

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir mensajes JSON
datos = df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Convertir la marca de tiempo
datos = datos.withColumn(
    "fecha",
    to_timestamp(from_unixtime(col("timestamp")))
)

# Calcular promedios por sensor cada minuto
resultados = datos.groupBy(
    window(col("fecha"), "1 minute"),
    col("sensor_id")
).agg(
    avg("temperature").alias("temperatura_promedio"),
    avg("humidity").alias("humedad_promedio")
)

# Mostrar resultados en consola
query = resultados.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
