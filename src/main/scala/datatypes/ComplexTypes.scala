package datatypes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object ComplexTypes extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("ComplexTypes").config("spark.master", "local[*]").getOrCreate()

  val moviesDf = session.read.option("inferSchema", "true").json("data/movies.json")

  // Dates
  println("working with dates")
  val moviesWithReleaseDates = moviesDf
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("Right_now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add and date_sub also available

  moviesWithReleaseDates.show()
  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()

  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stocksDf = session.read
    .schema(stocksSchema)
    .option("header", "true")
    .option("dateFormat", "MMM dd yyyy").csv("data/stocks.csv")
  stocksDf.show()
  stocksDf.describe().show()
  stocksDf.printSchema()

  // Structures
  println("working with structures")
  moviesDf
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
    .show()

  moviesDf
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross as US_Profit")
    .show()

  println("arrays")
  val moviesWithWords = moviesDf.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")).as("Array_size"),
    array_contains(col("Title_Words"), "Love")
  ).show()
}
