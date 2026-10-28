from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName('Tarea3').getOrCreate()
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
df = spark.read.format('csv').option('header','true').option('inferSchema','true').load(file_path)

df.printSchema()

df.show()

df.summary().show()

print("Dias con valor mayor a 5000")
dias = df.filter(F.col('VALOR') > 5000).select('VALOR', 'VIGENCIADESDE', 'VIGENCIAHASTA')
dias.show()

print("Valores ordenados de mayor a menor")
sorted_df = df.sort(F.col('VALOR').desc())
sorted_df.show()
