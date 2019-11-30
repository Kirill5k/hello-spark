package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions2 extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("ColumnsAndExpressions2")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val moviesDf = session.read.option("inferSchema", "true").json("data/movies.json")

  val totalGrossCol = moviesDf.col("US_Gross") + moviesDf.col("Worldwide_Gross")
  val extendedMoviesDf = moviesDf.withColumn("Total_Gross", totalGrossCol)

  extendedMoviesDf.describe().show()
  extendedMoviesDf.head(5).foreach(println)

  val goodComedyMovies = moviesDf.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  // moviesDf.filter("Major_Genre = 'Comedy' AND IMDB_Rating > 6")

  goodComedyMovies.head(5).foreach(println)

  session.stop()
}
