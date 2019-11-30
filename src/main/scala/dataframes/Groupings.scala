package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Groupings extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("Groupings")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val moviesDf = session.read.option("inferSchema", "true").json("data/movies.json")

  val totalMoviesCount = moviesDf.select(count("*"))
  val genresCount = moviesDf.select(count(col("Major_Genre"))) // all values except null
  genresCount.show()
  val distinctGenresCount = moviesDf.select(countDistinct(col("Major_Genre")))
  val genres = moviesDf.select("Major_Genre").distinct()
  genres.show()


  val meanRatingDf = moviesDf.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  val countByGenreDf = moviesDf
    .groupBy(col("Major_Genre")) // includes
    .count() // select count(*) from moviesDf group by Major_Genre

  val averageRatingByGenreDf = moviesDf
      .groupBy(col("Major_Genre"))
      .avg("IMDB_Rating")

  val aggregationsByGenreDf = moviesDf
      .groupBy(col("Major_Genre"))
      .agg(
        count("*").as("N_Movies"),
        avg("IMDB_Rating").as("Avg_Rating")
      ).orderBy(col("Avg_Rating"))


  val totalGrossCol = moviesDf.col("US_Gross") + moviesDf.col("Worldwide_Gross") + moviesDf.col("US_DVD_Sales")
  val allProfits = moviesDf.select(sum(totalGrossCol))

  val directorsCount = moviesDf.select(countDistinct("Director"))

  val meanGross = moviesDf.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  )

  val grossByDirectors = moviesDf
      .groupBy(col("Director"))
      .agg(
        avg("IMDB_Rating").as("Avg_Rating"),
        avg("US_Gross").as("Avg_US_Gross")
      ).orderBy(col("Avg_Rating").desc_nulls_last)

  session.stop()
}
