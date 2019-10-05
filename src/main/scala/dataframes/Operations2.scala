package dataframes

import dataframes.DatesAndTimestamps.df
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Operations2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder()
    .appName("Operations2")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import session.implicits._

  val df = session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/Netflix_2011_2016.csv")

  println(df.columns.toList)
  df.printSchema()
  df.show(5)
  df.describe()

  println("high/volume ratio")
  val dfWithHVRation = df.withColumn("HV Ratio", df("High") / df("Volume"))
  dfWithHVRation.show(5)

  println("peak high")
  df.select(max("High")).show()

  println("close mean")
  df.select(mean("Close")).show()

  println("min and max volume")
  df.select(min("Volume")).show()
  df.select(max("Volume")).show()

  println("amount of days when close was lower than $600")
  println(df.filter($"Close" < 600).count())

  println("% of time when High was greater than $500")
  val highGreater500 = df.filter($"High" > 600).count()
  val total = df.count()
  println(s"total = $total, highGreaterThan500 = $highGreater500, % = ${highGreater500 * 100 / total}")
  println()

  println("pearson correlation between High and Volume")
  df.select(corr("High", "Volume")).show(5)

  println("max high per year")
  val dfWithYear = df.withColumn("Year", year(df("Date")))
  dfWithYear
    .groupBy("Year")
    .max()
    .select($"Year", $"max(High)")
    .show(4)

  println("average close per month")
  val dfWithMonth = df.withColumn("Month", month(df("Date")))
  dfWithMonth
    .groupBy("Month")
    .mean()
    .select($"Month", $"avg(Close)")
    .show(4)
}
