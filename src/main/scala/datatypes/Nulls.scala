package datatypes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Nulls extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("Nulls").config("spark.master", "local[*]").getOrCreate()

  val moviesDf = session.read.option("inferSchema", "true").json("data/movies.json")

  // select the first non-null value
  println("select the first non-null value")
  moviesDf.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  ).show()

  // select data when value is not null
  moviesDf.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDf.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing or replacing nulls
  moviesDf.select("Title", "IMDB_Rating").na.drop()
  moviesDf.na.fill(0, List("IMBD_Rating", "Rotten_Tomatoes_Rating"))
  moviesDf.na.fill(Map(
    "Rotten_Tomatoes_Rating" -> 10,
    "IMDB_Rating" -> 0,
    "Director" -> "Unknown"
  ))

  moviesDf.selectExpr(
    "Title", "IMDB_Rating", "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nulliff", // returns null if 2 values are equal, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show

}
