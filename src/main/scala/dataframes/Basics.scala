package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Basics extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("Basic")
    .config("spark.master", "local[*]")
    .getOrCreate()

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
