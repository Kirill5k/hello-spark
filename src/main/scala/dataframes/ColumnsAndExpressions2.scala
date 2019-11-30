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

  val goodComedyMovies2 = moviesDf.select("Title","IMDB_Rating").where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  goodComedyMovies.head(5).foreach(println)

  val moviesProfitDf = moviesDf.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  val moviesProfitDf2 = moviesDf.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  session.stop()
}
