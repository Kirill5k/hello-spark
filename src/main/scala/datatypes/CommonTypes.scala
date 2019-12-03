package datatypes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("CommonTypes").config("spark.master", "local[*]").getOrCreate()

  val moviesDf = session.read.option("inferSchema", "true").json("data/movies.json")

  // literals
  println("adding a plain value to a DF")
  moviesDf.select(col("Title"), lit(47).as("plain_value")).show()
  println()

  val dramaFilter = col("Major_genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7
  val preferredFilter = dramaFilter and goodRatingFilter

  // booleans
  println("filtering with where")
  moviesDf.select("Title").where(preferredFilter).show()
  println()

  println("selecting with filter evaluated as value")
  val goodMoviesDf = moviesDf.select(col("Title"), preferredFilter.as("good_movie"))
  goodMoviesDf.show()
  goodMoviesDf.where("good_movie").show()
  println()


  // numbers
  println("correlations")
  val moviesAvgRatingsDf = moviesDf.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
  moviesAvgRatingsDf.show()
  println()

  val ratingCorrelationDf = moviesDf.select(corr(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating")).as("correlation"))
  println("corr via select", ratingCorrelationDf.head())
  println("corr via stat", moviesDf.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))
  ratingCorrelationDf.show()


  val carsDf = session.read.option("inferSchema", "true").json("data/cars.json")

  println("capitalization (can also use initcap, lower, upper)")
  carsDf.select(initcap(col("Name")))

  println("can select with contains")
  carsDf.select("*").where(col("Name").contains("volkswagen"))
  println("can select with regex as well")
  val wvDf = carsDf
    .select(col("Name"), regexp_extract(col("Name"), "volkswagen|wv", 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")
  wvDf.show()

  println("regex for replacing")
  wvDf.select(col("Name"), regexp_replace(col("Name"), "volkswagen|wv", "People's Car")).show()


  // Filtering via list of words

  def getCarNames: List[String] = List("BMW", "Ford")
  val filteredDf = carsDf
    .select(col("Name"), regexp_extract(col("Name"), getCarNames.map(_.toLowerCase).mkString("|"), 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  val carNameFilters = getCarNames.map(_.toLowerCase).map(col("Name").contains(_)).reduceLeft((acc, f) => acc or f)
  carsDf.select(col("Name")).where(carNameFilters).show()

}
