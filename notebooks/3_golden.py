# Databricks notebook source
from pyspark.sql.types import *

from pyspark.sql import functions as F

# COMMAND ----------

gold_schema = StructType([
    StructField("Series_Title", StringType(), True),
    StructField("Released_Year", IntegerType(), True),
    StructField("Certificate", StringType(), True),
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
    StructField("Primary_Genre", StringType(), True),
    StructField("Rating_Category", StringType(), True),
    StructField("Movie_age", IntegerType(), True)
])

# COMMAND ----------

df = spark.table("movie_rating.silver.slv_movie")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Popularity based on number of votes

# COMMAND ----------

df = df.withColumn(
    "Popularity_Category",
    F.when(F.col("No_of_votes")>= 500000, "Very Popural")
    .when(F.col("No_of_Votes")>= 200000, "Popular")
    .when(F.col("No_of_Votes")>= 100000, "Moderately Popular")
    .otherwise("Less popular")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Audience engagement

# COMMAND ----------

df = df.withColumn(
    "Audience_Engagement",
    F.when(F.col("No_of_Votes")>= 500000, "High")
     .when(F.col("No_of_Votes")>= 200000,  "Medium")
     .otherwise("Low")
)

# COMMAND ----------

#IMDB Rating vs Meta Score
#convert IMDB rating from 10-point scale to 100-point scale
df = df.withColumn(
    "Critic_Audience_gap",
    F.round(
        (F.col("IMDB_Rating")*10)- F.col("Meta_Score"),
        2
    )
)

# COMMAND ----------

# 6. Movie era
df = df.withColumn(
         "Movie_Era",
         F.when(F.col("Released_Year") < 1980, "Before 1980")
          .when(F.col("Released_Year") < 1990, "1980s")
          .when(F.col("Released_Year") < 2000, "1990s")
          .when(F.col("Released_Year") < 2010, "2000s")
          .when(F.col("Released_Year") < 2020, "2010s")
          .otherwise("2020s")
    )

# COMMAND ----------

 # 7. Overall movie success
df =df.withColumn(
        "Movie_Success",
        F.when(
            (F.col("IMDB_Rating") >= 8.0) &
            (F.col("No_of_Votes") >= 200000),
            "Highly Successful"
        )
        .when(
            (F.col("IMDB_Rating") >= 7.0) &
            (F.col("No_of_Votes") >= 100000),
            "Successful"
        )
        .when(
            F.col("IMDB_Rating") >= 7.0,
            "Well Rated"
        )
        .otherwise("Average")
    )


# COMMAND ----------

catalog_name = 'movie_rating'

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
    .saveAsTable(f"{catalog_name}.gold.gld_movie")


# COMMAND ----------

