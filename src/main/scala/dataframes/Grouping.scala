package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Grouping extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder()
    .appName("Grouping")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import session.implicits._

  val df = session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/Sales.csv")

  df.printSchema()

  df.groupBy("Company").mean().show()
  df.groupBy("Company").sum().show()

  df.select(countDistinct("Sales")).show()
  df.select(sumDistinct("Sales")).show()
  df.select(collect_set("Sales")).show()

  df.orderBy("Sales").show()
}
