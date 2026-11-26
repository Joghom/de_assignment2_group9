"""
Pipeline 2 - Best-value bundles per genre  (silver -> gold)

Sources : BigQuery dataset.bundle_items_clean, dataset.games_clean   (written by pipeline0_clean.py)
Process : join bundle items with games (normal store price) -> aggregate per bundle (savings)
          -> window: main genre per bundle -> window: rank bundles per genre by savings %
Sink    : BigQuery dataset.bundle_best_value

Question: "For each genre, which bundles save you the most compared to buying
the games separately?"
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def run_job(project_id, bucket, dataset, top_n):
    spark = SparkSession.builder.appName("SteamPipeline2_BundleValue").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("temporaryGcsBucket", bucket)
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    # Extract (already cleaned)
    items = spark.read.format("bigquery").load(f"{project_id}.{dataset}.bundle_items_clean")
    games = spark.read.format("bigquery").load(f"{project_id}.{dataset}.games_clean") \
        .select(F.col("id").alias("game_id"), F.col("price").alias("list_price"))

    # Transform
    # 1 value of each item = normal store price, or the bundle's item price if unknown
    items_priced = (items.join(games, items["item_id"] == games["game_id"], "left")
                    .withColumn("item_value",
                                F.coalesce("list_price", "item_discounted_price")))

    # 2 aggregate per bundle
    per_bundle = (items_priced
                  .groupBy("bundle_id", "bundle_name", "bundle_price", "bundle_final_price")
                  .agg(F.countDistinct("item_id").alias("num_games"),
                       F.round(F.sum("item_value"), 2).alias("separate_total"))
                  .where(F.col("separate_total") > 0)
                  .withColumn("savings", F.round(F.col("separate_total") - F.col("bundle_final_price"), 2))
                  .withColumn("savings_pct", F.round(F.col("savings") / F.col("separate_total") * 100, 1)))

    # 3 main genre of a bundle = most frequent genre among its games (window #1)
    genre_counts = (items.select("bundle_id", F.explode("item_genres").alias("genre"))
                    .groupBy("bundle_id", "genre").agg(F.count("*").alias("n")))
    w_main = Window.partitionBy("bundle_id").orderBy(F.desc("n"), "genre")
    main_genre = (genre_counts.withColumn("r", F.row_number().over(w_main))
                  .where(F.col("r") == 1).select("bundle_id", F.col("genre").alias("main_genre")))

    # 4 rank bundles inside each main genre by savings % (window #2)
    w_rank = Window.partitionBy("main_genre").orderBy(F.desc("savings_pct"), F.desc("savings"))
    result = (per_bundle.join(main_genre, "bundle_id", "inner")
              .withColumn("rank_in_genre", F.row_number().over(w_rank))
              .where(F.col("rank_in_genre") <= top_n)
              .select("main_genre", "rank_in_genre", "bundle_id", "bundle_name", "num_games",
                      "separate_total", "bundle_final_price", "savings", "savings_pct")
              .orderBy("main_genre", "rank_in_genre"))

    result.show(30, truncate=False)

    # Load
    (result.write.format("bigquery")
     .option("table", f"{project_id}.{dataset}.bundle_best_value")
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
