# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql import functions as f

# COMMAND ----------

spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()


# COMMAND ----------

catalog_name = 'movie_rating'

# COMMAND ----------

#Define schema for the data file 

imdb_schema = StructType([

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

    StructField("No_of_Votes", IntegerType(), True),

    StructField("Gross", LongType(), True)

])

# COMMAND ----------

raw_data_path = "/Volumes/pyspark_cata/default/movie_rating"
df = spark.read\
    .option('header', "true")\
        .option('delimeter', ",")\
            .schema(imdb_schema)\
                .csv(raw_data_path)


# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
    .saveAsTable(f"{catalog_name}.bronze.brz_movie")


# COMMAND ----------

