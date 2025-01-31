from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    count = batch_df.count()
    print(f"Number of transactions in this batch: {count}")
    batch_df.show()

# Créer une session Spark avec le support pour Kafka et MinIO
spark = SparkSession.builder \
 .appName("KafkaSparkConsumer") \
 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4") \
 .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
 .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
 .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
 .config("spark.hadoop.fs.s3a.access.key", "minio") \
 .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
 .config("spark.hadoop.fs.s3a.path.style.access", "true") \
 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
 .getOrCreate()
# Définir le schéma simplifié correspondant à votre producteur
schema = StructType([
StructField("id_transaction", StringType(), True),
StructField("type_transaction", StringType(), True),
StructField("montant", DoubleType(), True),
StructField("devise", StringType(), True),
StructField("date", StringType(), True),
StructField("moyen_paiement", StringType(), True)
])
# Lire les messages Kafka
raw_kafka_df = spark.readStream.format("kafka") \
 .option("kafka.bootstrap.servers", "localhost:9092") \
 .option("subscribe", "transactions") \
 .load()
# Extraire la colonne 'value' (messages JSON sous forme de chaîne)
kafka_values_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_data")
# Convertir les messages JSON en colonnes structurées (DataFrame)
transactions_df = kafka_values_df.select(
from_json(col("json_data"), schema).alias("data")
).select("data.*")
# Filtrer les données invalides
filtered_df = transactions_df.filter(col("moyen_paiement") != "erreur")
filtered_df = transactions_df.filter(col("montant").isNotNull())
filtered_df = transactions_df.filter(col("id_transaction").isNotNull())
# Appliquer les transformations
transformed_df = filtered_df.withColumn("montant_original", col("montant"))
transformed_df = transformed_df.withColumn("devise_original", col("devise"))
transformed_df = transformed_df.withColumn("montant_eur", col("montant") * lit(0.85))
transformed_df = transformed_df.withColumn("devise", lit("EUR"))
transformed_df = transformed_df.withColumn("timezone", lit("Europe/Paris"))
transformed_df = transformed_df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
# Sélectionner les colonnes finales
final_df = transformed_df.select(
"id_transaction",
"type_transaction",
"montant_original",
"devise_original",
"montant_eur",
"devise",
"date",
"timezone",
"moyen_paiement"
)


# Écrire les données transformées dans MinIO au format Parquet
query = final_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://warehouse/transactions") \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/transactions") \
    .start()
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Arrêt demandé par l'utilisateur.")
finally:
    query.stop()
    spark.stop()
    print("Streaming et Spark arrêtés correctement.")