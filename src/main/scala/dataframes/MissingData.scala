package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MissingData extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder()
    .appName("MissingData")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import session.implicits._

  val df = session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/ContainsNull.csv")

  df.printSchema()
  df.show()

  // drop nulls. can set limits
  df.na.drop().show()

  // fill with default value
  val salesMean = df.select(mean("Sales")).first().get(0).asInstanceOf[Double]
  df.na.fill(salesMean, Array("Sales")).na.fill("Boris").show()
}
