# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a399aec-f1f5-40d3-9bb8-d41a80c0b5dd",
# META       "default_lakehouse_name": "OR",
# META       "default_lakehouse_workspace_id": "28af571b-e594-4ba4-bba7-9cc05da270ce",
# META       "known_lakehouses": [
# META         {
# META           "id": "1a399aec-f1f5-40d3-9bb8-d41a80c0b5dd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialisation Spark (inutile si déjà dans un notebook Databricks)
spark = SparkSession.builder.getOrCreate()

# Données source
exerBud_raw = ["2022-2023", "2023-2024", "2024-2025"]
cibles_Ministere = [1364.970766, 1364.970766, 1499.1]  # À adapter si besoin

# Création DataFrame avec schéma explicite
schema = StructType([
    StructField("exercice", StringType(), True),
    StructField("cible_etp", DoubleType(), True)
])
df = spark.createDataFrame(zip(exerBud_raw, cibles_Ministere), schema=schema)

# Sauvegarde dans la table Delta
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("annee_scolaire")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
