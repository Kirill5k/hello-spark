package practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestDeploy {

  /**
   * Movies.json as args(0)
   * GoodComedies.json as args(1)
   * where good comedy = genre === Comedy and IMDB > 6.5
   * @param args
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("need input path and output path")
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("Test deploy app").getOrCreate()

    val moviesDf = spark.read.option("inferSchema", "true").json(args(0))
    val goodComediesDf = moviesDf.select(
      col("Title"),
      col("IMDB_Rating").as("Rating"),
      col("Release_Date").as("Release")
    ).where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5)

    goodComediesDf.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }
}