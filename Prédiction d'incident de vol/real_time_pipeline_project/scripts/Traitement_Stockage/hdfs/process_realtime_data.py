from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType ,StructField

# Configurer la session Spark
spark = SparkSession.builder \
    .appName("Kafka-to-HDFS-DataLake") \
    .getOrCreate()

# Configuration Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topics = {
    "geographie": "geographie_topic",
    "incidents": "incidents_vol_topic",
    "population": "population_topic",
    "trafic": "trafic_topic",
    "meteo": "meteo_topic"
}

# Définir les schémas pour chaque type de données
schema_geographie = StructType([
    StructField("quartier", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("superficie", FloatType(), True)
])

schema_incidents = StructType([
    StructField("id_incident", IntegerType(), True),
    StructField("quartier", StringType(), True),
    StructField("type_vol", StringType(), True),
    StructField("valeur_bien", FloatType(), True),
    StructField("etat", StringType(), True),
    StructField("age_victime", IntegerType(), True),
    StructField("sexe_victime", StringType(), True),
    StructField("voleur_arrete", StringType(), True),
    StructField("distance_agence_police", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

schema_population = StructType([
    StructField("quartier", StringType(), True),
    StructField("densite_population", FloatType(), True),
    StructField("taux_chomage", FloatType(), True),
    StructField("niveau_vie", StringType(), True),
    StructField("timestamp", StringType(), True)
])

schema_trafic = StructType([
    StructField("id_trafic", IntegerType(), True),
    StructField("quartier", StringType(), True),
    StructField("nombre_vehicules", IntegerType(), True),
    StructField("niveau_trafic", StringType(), True),
    StructField("timestamp", StringType(), True)
])

schema_meteo = StructType([
    StructField("id_meteo", IntegerType(), True),
    StructField("quartier", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("precipitations", FloatType(), True),
    StructField("vent", FloatType(), True),
    StructField("humidite", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

schemas = {
    "geographie": schema_geographie,
    "incidents": schema_incidents,
    "population": schema_population,
    "trafic": schema_trafic,
    "meteo": schema_meteo
}

# Fonction pour lire Kafka et écrire dans HDFS
def store_raw_data(topic_name, schema, hdfs_path):
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_name) \
        .load()

    # Décoder les messages Kafka (aucun nettoyage, aucun enrichissement)
    df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Écrire les données brutes dans HDFS
    df_parsed.writeStream \
        .format("parquet") \
        .option("path", hdfs_path) \
        .option("checkpointLocation", f"{hdfs_path}/checkpoints") \
        .outputMode("append") \
        .start()

# Définir les chemins HDFS pour chaque type de données
paths = {
    "geographie": "hdfs://localhost:9000/data_lake/raw/geographie",
    "incidents": "hdfs://localhost:9000/data_lake/raw/incidents",
    "population": "hdfs://localhost:9000/data_lake/raw/population",
    "trafic": "hdfs://localhost:9000/data_lake/raw/trafic",
    "meteo": "hdfs://localhost:9000/data_lake/raw/meteo"
}

# Lancer le stockage brut pour chaque topic Kafka
for data_type, topic in kafka_topics.items():
    store_raw_data(topic_name=topic, schema=schemas[data_type], hdfs_path=paths[data_type])

# Attendre la fin des streams
spark.streams.awaitAnyTermination()
