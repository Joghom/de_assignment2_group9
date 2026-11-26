"""
Pipeline 1 - Top games per genre by review activity  (silver -> gold)

Sources : BigQuery dataset.reviews_clean, dataset.games_clean   (written by pipeline0_clean.py)
Process : aggregate reviews per game -> join with games -> explode genres
          -> window: rank games inside each genre -> keep top N
Sink    : BigQuery dataset.genre_top_games

Question: "For each genre, which games get the most reviews, how long do their
reviewers play them, and what share of the genre's reviews do they get?"
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def run_job(project_id, bucket, dataset, top_n):
    spark = SparkSession.builder.appName("SteamPipeline1_GenreTopGames").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("temporaryGcsBucket", bucket)
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    # Extract (already cleaned)
    reviews = spark.read.format("bigquery").load(f"{project_id}.{dataset}.reviews_clean")
    games = spark.read.format("bigquery").load(f"{project_id}.{dataset}.games_clean")

    # Transform
    # 1 aggregate per game first (shrinks the data before the join)
    per_game = (reviews.groupBy("product_id")
                .agg(F.count("*").alias("num_reviews"),
                     F.round(F.avg("hours"), 1).alias("avg_hours_played"),
                     F.round(F.avg("text_length"), 0).alias("avg_review_length"),
                     F.min("review_date").alias("first_review"),
                     F.max("review_date").alias("last_review")))

    # 2 join with games, one row per genre
    joined = (per_game.join(games, per_game["product_id"] == games["id"], "inner")
              .where(F.size("genres") > 0)
              .select("id", "app_name", "price", F.explode("genres").alias("genre"),
                      "num_reviews", "avg_hours_played", "avg_review_length",
                      "first_review", "last_review"))

    # 3 window functions: rank inside the genre + genre totals
    w_rank = Window.partitionBy("genre").orderBy(F.desc("num_reviews"), F.desc("avg_hours_played"))
    w_genre = Window.partitionBy("genre")

    result = (joined
              .withColumn("rank_in_genre", F.row_number().over(w_rank))
              .withColumn("genre_total_reviews", F.sum("num_reviews").over(w_genre))
              .withColumn("share_of_genre_pct",
                          F.round(F.col("num_reviews") / F.col("genre_total_reviews") * 100, 2))
              .where(F.col("rank_in_genre") <= top_n)
              .orderBy("genre", "rank_in_genre"))

    result.show(30, truncate=False)

    # Load
    (result.write.format("bigquery")
     .option("table", f"{project_id}.{dataset}.genre_top_games")
     .mode("overwrite").save())

    spark.stop()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--project_id", default="assignment2-515810")
    p.add_argument("--bucket", default="assign2_data")
    p.add_argument("--dataset", default="dataset")
    p.add_argument("--top_n", type=int, default=3)
    return vars(p.parse_args())


if __name__ == "__main__":
    run_job(**parse_args())
