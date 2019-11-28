package mllib.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object LinearRegression extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("LinearRegression")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val trainingData = session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/clean-ecommerce.csv")


  session.stop()
}
