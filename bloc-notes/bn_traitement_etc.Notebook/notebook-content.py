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
# META         },
# META         {
# META           "id": "506d295c-3dbf-4271-bfa0-37bfd2e756d7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Importation des librairies

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, regexp_replace, lit, sum, first, concat_ws, to_date\
, rank
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
import unicodedata

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Initialisation des variables globales

# CELL ********************

# Initialisation de variables **********************************************************************
Interface   = "abfss://core_fabric@onelake.dfs.fabric.microsoft.com/ld_paie.Lakehouse/Tables/"
Or          = "abfss://Fabric_RH_ETC@onelake.dfs.fabric.microsoft.com/OR.Lakehouse/Tables/"
Bronze      = "abfss://Fabric_RH_ETC@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse/"

source      = (Bronze + "/Files/etc")
destination = Or + "/Tables/etc/"
fichiers_csv = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Liste des fonctions

# CELL ********************

## Fonction pour ajouter des colonnes **************************************************************
def ajout_colonnes(df):
  # Références locales pour accéder plus rapidement aux colonnes souvent utilisées
  corps_col = F.col("Corps d'emploi PERCOS")
  stat_col = F.col("Statut d'engagement Percos")
  chq_date = F.col("chq_date")
  year_col = F.year(chq_date)
  month_col = F.month(chq_date)

  # Ajout des colonnes calculées au DataFrame
  return (
    df
    # Calcul du nombre d'ETC (équivalent temps complet) à partir des heures rémunérées
    .withColumn("calculETC", F.col("hrs_rem") / 1826.3)
    # Création d'un identifiant combiné pour le corps d'emploi
    .withColumn("corp_empl", F.concat_ws("-", corps_col, F.col("DESCR")))
    # Création d'un identifiant combiné pour le statut d'engagement
    .withColumn("stat_engv2", F.concat_ws("-", stat_col, F.col("descr_stat_eng")))
    # Renommer colonne 
    .withColumnRenamed("Numéro d'organisme", "id_org")

    # Classification des catégories d'employés selon le code du corps d'emploi
    .withColumn(
      "cat_empl",
      F.when(corps_col.startswith('1'), 'Gestionnaire')
        .when(corps_col.startswith('2'), 'Professionnel(le)')
        .when(corps_col.startswith('3'), 'Enseignant(e)')
        .when(corps_col.isin('4207', '4515', '4208', '4223', '1230', '4287', '4285', '4286'\
        , '4288'), 'Soutien service direct à l’élèves')
        .when(corps_col.startswith('4'), 'Soutien administratif')
        .when(corps_col.startswith('5'), 'Soutien manuel')
        .otherwise('Autres')
    )
    # Calcul de l'exercice fiscal (année scolaire de juillet à juin)
    .withColumn(
      "exercice",
      F.when(month_col >= 7,
        F.concat_ws("-", year_col, (year_col + 1).cast("string")))
      .when(month_col < 7,
        F.concat_ws("-", (year_col - 1).cast("string"), year_col))
        .otherwise(F.lit("n/d"))
    )
  )

## Fonction traiter les CSV ************************************************************************
def traiter_df(df_csv, df_corpsEmpl, df_stat_eng):
  # Nettoyage et préparation initiale des colonnes du fichier source
  df_csv = (
    df_csv
    # Conversion de la date du chèque au format date
    .withColumn("chq_date", F.to_date(F.col("Date du chèque")))
    # Conversion du matricule en chaîne de caractères
    .withColumn("matr", F.col("Matricule").cast("string"))
    # Remplacement des virgules par des points et conversion en float pour les heures rémunérées
    .withColumn("hrs_rem", F.regexp_replace(F.col("Nombre d'heures rémunérées"), ",", ".")\
    .cast("double"))
  )

  # Jointure avec les tables de référence des corps d'emploi et des statuts d'engagement
  df = (
    df_csv
    .join(df_corpsEmpl, df_csv["Corps d'emploi PERCOS"] == df_corpsEmpl["CORP_EMPL"], "left")
    .join(df_stat_eng, df_csv["Statut d'engagement Percos"] == df_stat_eng["stat_eng"], "left")
  )

  # Ajout des colonnes calculées à l'aide de la fonction ajout_colonnes
  df = ajout_colonnes(df)

  # Création d'une fenêtre pour numéroter les périodes de paie dans chaque exercice fiscal
  window_periode = Window.partitionBy("exercice").orderBy("chq_date")
  df = df.withColumn("periode", F.dense_rank().over(window_periode))

  # Sélection des colonnes finales à retourner
  return df.select(
    "id_org",
    "exercice",
    "chq_date",
    "cat_empl",
    "periode",
    "hrs_rem",
    "calculETC",
    "corp_empl",
    "stat_engv2"
  )
 
## Fonction | Aller lire tous les csv **************************************************************
def list_csv_recursive(path):
  fichiers = []
  for f in mssparkutils.fs.ls(path):  # Liste le contenu du dossier
    if f.isDir:
      # Si c'est un sous-dossier, appel récursif
      fichiers.extend(list_csv_recursive(f.path))
    elif f.path.lower().endswith(".csv"):
      # Si c'est un fichier CSV, on l'ajoute à la liste
      fichiers.append(f.path)
  return fichiers

## Fonction | Aller lire tous les csv **************************************************************
def lire_csv_spark(
  spark, fichiers_csv, delimiter=";", encoding="windows-1256", 
  multiline="true", infer_schema="true", header="true"
):
  return (
    spark.read.format("csv")
    .option("header", header)              # Indique si la première ligne est un en-tête
    .option("inferSchema", infer_schema)   # Déduit automatiquement le type des colonnes
    .option("delimiter", delimiter)        # Définit le séparateur de champs
    .option("encoding", encoding)          # Définit l'encodage du fichier
    .option("multiline", multiline)        # Gère les champs sur plusieurs lignes
    .load(fichiers_csv)                    # Charge le(s) fichier(s) CSV
  )
  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exécution du code

# CELL ********************

# Récupération récursive de tous les fichiers CSV (dans le dossier source et ses sous-dossiers)
fichiers_csv = list_csv_recursive(source)

# Lecture des fichiers CSV dans un DataFrame Spark avec les options recommandées
df_csv = lire_csv_spark(spark, fichiers_csv)

# Lecture des tables de référence (corps d'emploi et statuts d'engagement) au format Delta
df_corpsEmpl = spark.read.format("delta").load(Interface + "tb_paie_corp_empl")
df_stat_eng = spark.read.format("delta").load(Interface + "tb_paie_stat_eng")

# Traitement principal du DataFrame avec les tables de référence
df_traite = traiter_df(df_csv, df_corpsEmpl, df_stat_eng)

# Sauvegarde du résultat final dans une table Delta (option overwriteSchema à activer si le 
# schéma peut évoluer)
df_traite.write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
.saveAsTable("donnees_traitees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
