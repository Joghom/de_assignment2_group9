from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode, split, concat, col, lit, desc

sparkConf = SparkConf()
sparkConf.setAppName("BigqueryExample")
# create the spark session, which is the entry point to Spark SQL engine.
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()
# Load data from BigQuery.
df = spark.read \
  .format("bigquery") \
  .load("assignment2-515810.labdataset.retaildata")
df.printSchema()
df.show(4)
words = df.where(df.Country == "France")
words.show(100)
words = df.where(df.Country == "France").select(
            explode(
                split(col("Description"), " ")
            ).alias("word")
        )
words.show(100)

ordered_word_count = words.groupby(words.word).count().orderBy(col("count").desc())
print(ordered_word_count.count())
ordered_word_count.show(100)
# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
bucket = "dataproc-temp-us-central1-712572237982-rwktgmp7"
spark.conf.set('temporaryGcsBucket', bucket)
# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
# Saving the data to BigQuery
ordered_word_count.write.format('bigquery') \
  .option('table', 'your_project_id.labdataset.wordcounts') \
  .mode("append") \
  .save()
