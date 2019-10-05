package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Operations extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder()
    .appName("Operations")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import session.implicits._

  val df = session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/Netflix_2011_2016.csv")

  df.printSchema()

  // user === for equality
  df.filter($"Close" > 480 && $"High" === 575.000023).show(10)
//  df.filter("Close > 480 AND High = 575.000023").show(10)

  val chLow = df.filter($"Close" > 480 && $"High" > 570).collect()
  println(chLow.length)

  // correlation
  //  Correlation.corr(df, "High", "Low").show()
  // more functions here: http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
  df.select(corr("High", "Low")).show()
}
