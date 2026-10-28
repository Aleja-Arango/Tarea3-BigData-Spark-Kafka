# Tarea3-BigData-Spark-Kafka
Procesamiento batch y en tiempo real de datos de temperatura y humedad con Apache Spark, Python y Kafka.

Este proyecto tiene como objetivo analizar registros de temperatura y humedad mediante Apache Spark y Python, utilizando tanto el procesamiento por lotes (batch) como la simulación del procesamiento de datos en tiempo real con Apache Kafka y Spark Streaming.

Conjunto de datos:
Se utilizó el dataset Occupancy Detection, disponible en UCI Machine Learning Repository.

Se procesaron 8.143 registros del archivo datatraining.txt, seleccionando las variables de fecha, temperatura y humedad.

Fuente: https://archive.ics.uci.edu/dataset/357/occupancy-detection

Herramientas utilizadas:
Python 3
Apache Spark 3.5.8
Apache Kafka 3.9.2
PySpark
Matplotlib
Ubuntu y VirtualBox
Procesamiento batch

Se desarrolló el archivo analisis_sensores.py para realizar la carga, limpieza, transformación y análisis de los registros.

Para ejecutarlo, desde la carpeta que contiene el dataset:
spark-submit --master local[*] analisis_sensores.py

Resultados obtenidos:
Registros procesados: 8.143
Temperatura promedio: 20,62 °C
Temperatura mínima: 19 °C
Temperatura máxima: 23,18 °C
Humedad promedio: 25,73 %
Humedad mínima: 16,745 %
Humedad máxima: 39,1175 %

Procesamiento en tiempo real:
Se utilizó Apache Kafka para recibir mediciones simuladas de sensores mediante el topic sensor_data.

El archivo kafka_producer.py genera y envía registros de temperatura y humedad cada segundo.

Para ejecutarlo, con Kafka previamente iniciado:
python3 kafka_producer.py

El archivo spark_streaming_consumer.py recibe los mensajes y calcula promedios por sensor en ventanas de un minuto.

spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
