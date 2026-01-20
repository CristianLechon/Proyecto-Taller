from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Configuración optimizada para 40GB en entorno local
spark = (
    SparkSession.builder.appName("Steam Data ETL")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# 1. INGESTIÓN DESDE HDFS
input_path = "hdfs://namenode:9000/user/root/steam_project/data/all_reviews.csv"
print(f"--- Leyendo desde HDFS: {input_path} ---")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# 2. TRANSFORMACIÓN
# Seleccionamos SOLO la columna 'voted_up' y nos aseguramos que no sea nula
print("--- Filtrando datos ---")
final_df = df.select(
    regexp_replace(col("game"), ",", "").alias("game_clean"), col("voted_up")
).filter(col("voted_up").isNotNull())

# 3. CARGA A HDFS
output_path = "hdfs://namenode:9000/user/root/steam_project/clean_data"
print(f"--- Guardando limpio en: {output_path} ---")

# Guardamos. Al ser solo 1 columna, el archivo tendrá líneas con solo "1" o "0"
final_df.write.mode("overwrite").csv(output_path)

print("¡Proceso finalizado con éxito!")
spark.stop()
