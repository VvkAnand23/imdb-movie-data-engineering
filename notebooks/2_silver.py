# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

silver_schema = StructType([
 StructField("Poster_Link", StringType(), True),
    StructField("Series_Title", StringType(), True),
    StructField("Released_Year", IntegerType(), True),
    StructField("Certificate", StringType(), True),
    StructField("Runtime", IntegerType(), True),
    StructField("Genre", StringType(), True),
    StructField("IMDB_Rating", DoubleType(), True),
    StructField("Overview", StringType(), True),
    StructField("Meta_score", DoubleType(), True),
    StructField("Director", StringType(), True),
    StructField("Star1", StringType(), True),
    StructField("Star2", StringType(), True),
    StructField("Star3", StringType(), True),
    StructField("Star4", StringType(), True),
    StructField("No_of_Votes", LongType(), True),
    StructField("Gross", LongType(), True)   
])

# COMMAND ----------

bronze_data_path = "movie_rating.bronze.brz_movie"
df = spark.read\
    .option("header", "true")\
    .option("delimiter", ",")\
    .schema(silver_schema)\
    .table(bronze_data_path)


# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df = df.withColumn(
    "Released_Year",
    F.col("Released_Year").cast("int")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Null value column

# COMMAND ----------

df = df.drop("Runtime", "Gross")

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Clean string columns

# COMMAND ----------

columns_to_trim = [
    "Series_Title",
    "Certificate",
    "Genre",
    "Director",
    "Star1",
    "Star2",
    "Star3",
    "Star4",
    "Overview",    
]

for c in columns_to_trim:
    df = df.withColumn(c, F.trim(F.col(c)))

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL value

# COMMAND ----------

df = df.withColumn(
    "Certificate",
    F.coalesce(F.col("Certificate"), F.lit("Not Rated"))
)
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Validate IMDB_rating

# COMMAND ----------

# MAGIC %md
# MAGIC Validate No_of_Votes

# COMMAND ----------

df = df.filter(
    F.col("No_of_Votes") >= 0
)
display(df.limit(5)) 

# COMMAND ----------

# MAGIC %md
# MAGIC 12. Create Primary_Genre

# COMMAND ----------

df = df.withColumn(
    "Primary_Genre",
    F.trim(F.split(F.col("Genre"), ",")[0])
)

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df = df.withColumn(
    "Rating_Category",
    F.when(F.col("IMDB_Rating") >= 8.0, "Excellent")
     .when(F.col("IMDB_Rating") >= 7.0, "Good")
     .otherwise("Average")
)

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df = df.withColumn(
    "Movie_age",
    F.lit(2026) - F.col("Released_Year")
)
display(df2.limit(5))

# COMMAND ----------

catalog_name = 'movie_rating'

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
    .saveAsTable(f"{catalog_name}.silver.slv_movie")


# COMMAND ----------

