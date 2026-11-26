"""
Pipeline 0 - Data cleaning (bronze -> silver)

Reads the RAW tables loaded with `bq load` and writes CLEANED tables that
pipelines 1 and 2 use. Also appends row counts to dataset.data_quality_log.

  raw (bronze)        cleaned (silver)
  dataset.games    -> dataset.games_clean
  dataset.reviews  -> dataset.reviews_clean
  dataset.bundles  -> dataset.bundle_items_clean   (one row per game in a bundle)

Cleaning rules
  games   : trim text, empty text -> NULL, drop rows without id, remove duplicate ids
            (keep the most complete row), app_name falls back to title,
            price/discount_price: "Free to Play"/"Free" -> 0, "$4.99" -> 4.99,
            missing price + "Free to Play" tag -> 0, release_date -> DATE (invalid -> NULL),
            metascore -> INT, sentiment like "3 user reviews" -> NULL (not a real rating),
            genres/tags trimmed, empty entries removed
  reviews : trim, drop rows without product_id or username, hours -> DOUBLE,
            negative or absurd hours (> 100,000) -> removed, date -> DATE,
            dates before Steam's launch (2003-09-12) or in the future -> removed,
            empty text -> NULL, add text_length, remove exact duplicates,
            keep only reviews of games that exist in games_clean (left semi join)
  bundles : explode items, parse "$" prices, item genre string -> array,
            drop items without item_id, drop bundles without a valid final price,
            final price > original price -> removed (inconsistent), remove duplicate items
"""
import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

STEAM_LAUNCH = "2003-09-12"
MAX_HOURS = 100000


def clean_text(c):
    """trim; empty string -> NULL"""
    t = F.trim(c.cast("string"))
    return F.when(t == "", None).otherwise(t)


def clean_array(c):
    """trim every element and drop empty ones"""
    return F.filter(F.transform(c, lambda x: F.trim(x)), lambda x: x != "")


def parse_price(c):
    """'Free to Play' -> 0.0, '$1,234.56' -> 1234.56, anything else -> NULL"""
    s = F.lower(F.trim(c.cast("string")))
    return (F.when(s.contains("free"), F.lit(0.0))
            .otherwise(F.regexp_replace(s, r"[^0-9.]", "").cast("double")))


def clean_games(raw):
    g = raw.select(
        clean_text(F.col("id")).alias("id"),
        F.coalesce(clean_text(F.col("app_name")), clean_text(F.col("title"))).alias("app_name"),
        clean_text(F.col("developer")).alias("developer"),
        clean_text(F.col("publisher")).alias("publisher"),
        clean_array(F.col("genres")).alias("genres"),
        clean_array(F.col("tags")).alias("tags"),
        clean_array(F.col("specs")).alias("specs"),
        parse_price(F.col("price")).alias("price"),
        parse_price(F.col("discount_price")).alias("discount_price"),
        F.to_date(F.col("release_date").cast("string")).alias("release_date"),
        F.col("metascore").cast("string").cast("int").alias("metascore"),
        clean_text(F.col("sentiment")).alias("sentiment"),
        F.col("early_access").cast("boolean").alias("early_access"),
    ).where(F.col("id").isNotNull())

    g = (g
         # no price but tagged Free to Play -> free
         .withColumn("price", F.when(F.col("price").isNull() &
                                     F.array_contains(F.col("tags"), "Free to Play"), F.lit(0.0))
                     .otherwise(F.col("price")))
         # "3 user reviews" is a count, not a rating
         .withColumn("sentiment", F.when(F.col("sentiment").contains("user review"), None)
                     .otherwise(F.col("sentiment")))
         # a discount price higher than the price makes no sense
         .withColumn("discount_price", F.when(F.col("discount_price") > F.col("price"), None)
                     .otherwise(F.col("discount_price"))))

    # remove duplicate ids: keep the row with the most filled-in fields
    fields = ["app_name", "developer", "publisher", "price", "release_date", "metascore", "sentiment"]
    completeness = sum(F.when(F.col(f).isNotNull(), 1).otherwise(0) for f in fields)
    w = Window.partitionBy("id").orderBy(F.desc("completeness"))
    return (g.withColumn("completeness", completeness)
            .withColumn("rn", F.row_number().over(w))
            .where(F.col("rn") == 1)
            .drop("rn", "completeness"))


def clean_reviews(raw, games_clean):
    r = raw.select(
        clean_text(F.col("product_id")).alias("product_id"),
        clean_text(F.col("username")).alias("username"),
        F.col("hours").cast("double").alias("hours"),
        F.col("products").cast("int").alias("user_products"),
        F.to_date(F.col("date").cast("string")).alias("review_date"),
        clean_text(F.col("text")).alias("text"),
        F.col("early_access").cast("boolean").alias("early_access"),
    )
    r = (r.where(F.col("product_id").isNotNull() & F.col("username").isNotNull())
         .where(F.col("hours").isNull() | F.col("hours").between(0, MAX_HOURS))
         .where(F.col("review_date").isNull() |
                F.col("review_date").between(F.lit(STEAM_LAUNCH).cast("date"), F.current_date()))
         .withColumn("text_length", F.coalesce(F.length("text"), F.lit(0)))
         .dropDuplicates(["product_id", "username", "review_date", "text"]))

    # referential integrity: keep only reviews for games we know (left semi join)
    return r.join(games_clean.select("id"), r["product_id"] == games_clean["id"], "left_semi")


def clean_bundles(raw):
    b = raw.select(
        clean_text(F.col("bundle_id")).alias("bundle_id"),
        clean_text(F.col("bundle_name")).alias("bundle_name"),
        parse_price(F.col("bundle_price")).alias("bundle_price"),
        parse_price(F.col("bundle_final_price")).alias("bundle_final_price"),
        F.explode(F.col("items")).alias("item"),
    )
    items = b.select(
        "bundle_id", "bundle_name", "bundle_price", "bundle_final_price",
        clean_text(F.col("item.item_id")).alias("item_id"),
        clean_text(F.col("item.item_name")).alias("item_name"),
        clean_array(F.split(F.col("item.genre"), ",")).alias("item_genres"),
        parse_price(F.col("item.discounted_price")).alias("item_discounted_price"),
    )
    return (items
            .where(F.col("bundle_id").isNotNull() & F.col("item_id").isNotNull())
            .where(F.col("bundle_final_price").isNotNull() & (F.col("bundle_final_price") >= 0))
            .where(F.col("bundle_price").isNull() |
                   (F.col("bundle_final_price") <= F.col("bundle_price")))
            .dropDuplicates(["bundle_id", "item_id"]))


def run_job(project_id, bucket, dataset):
    spark = SparkSession.builder.appName("SteamPipeline0_Clean").getOrCreate()
    # Spark 4 has ANSI mode on: a bad cast would crash the job. We want NULL instead.
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("temporaryGcsBucket", bucket)
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    def read(table):
        return spark.read.format("bigquery").load(f"{project_id}.{dataset}.{table}")

    def write(df, table, mode="overwrite"):
        df.write.format("bigquery").option("table", f"{project_id}.{dataset}.{table}") \
            .mode(mode).save()

    games_raw, reviews_raw, bundles_raw = read("games"), read("reviews"), read("bundles")

    games_clean = clean_games(games_raw).cache()
    reviews_clean = clean_reviews(reviews_raw, games_clean)
    bundle_items_clean = clean_bundles(bundles_raw)

    write(games_clean, "games_clean")
    write(reviews_clean, "reviews_clean")
    write(bundle_items_clean, "bundle_items_clean")

    # data quality log
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    stats = [
        ("games", games_raw.count(), read("games_clean").count()),
        ("reviews", reviews_raw.count(), read("reviews_clean").count()),
        ("bundles (rows = bundles raw, items clean)", bundles_raw.count(),
         read("bundle_items_clean").count()),
    ]
    log = spark.createDataFrame(
        [(run_ts, t, raw_n, clean_n) for t, raw_n, clean_n in stats],
        "run_ts string, source_table string, rows_raw long, rows_clean long")
    log.show(truncate=False)
    write(log, "data_quality_log", mode="append")

    spark.stop()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--project_id", default="assignment2-515810")
    p.add_argument("--bucket", default="assign2_data")
    p.add_argument("--dataset", default="dataset")
    return vars(p.parse_args())


if __name__ == "__main__":
    run_job(**parse_args())
