package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object Basics extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("Basic")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType, nullable = false),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val firstDf = session.read
    .schema(carsSchema)
    .format("json")
    .option("inferSchema", "true")
    .load("data/cars.json")

  firstDf.describe().show()
  firstDf.printSchema()
  println(firstDf.columns.toList)
  firstDf.head(5).foreach(println)

  val netflixDf = session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/Netflix_2011_2016.csv")

  netflixDf.describe().show()
  netflixDf.printSchema()
  println(netflixDf.columns.toList)
  netflixDf.head(5).foreach(println)

  // selecting column
  netflixDf.select("Date", "Volume").show(4)

  val updatedDf = netflixDf.withColumn("HighPlusLow", netflixDf("High") + netflixDf("Low"))

  updatedDf.printSchema()

  session.stop()
}
