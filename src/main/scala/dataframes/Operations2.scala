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

  println("columns")
  println(df.columns.toList)
  println("scheme")
  df.printSchema()
  println("first 5 columns")
  df.show(5)
  println("dataframe description")
  df.describe().show()

  println("high/volume ratio")
  val dfWithHVRatio = df.withColumn("HV Ratio", df("High") / df("Volume"))
  dfWithHVRatio.show(5)

  println("peak high")
//  df.select(max("High")).show()
  df.orderBy($"High".desc).show(1)

  println("close mean")
  df.select(mean("Close")).show()

  println("min and max volume")
  df.select(min("Volume")).show()
  df.select(max("Volume")).show()

  println("amount of days when close was lower than $600")
  println(df.filter($"Close" < 600).count())

  println("% of time when High was greater than $500")
  val highGreater500 = df.filter($"High" > 500).count()
  val total = df.count()
  println(s"total = $total, highGreaterThan500 = $highGreater500, % = ${highGreater500 * 100.00 / total}")
  println()

  println("pearson correlation between High and Volume")
  df.select(corr("High", "Volume")).show(5)

  println("max high per year")
  val dfWithYear = df.withColumn("Year", year(df("Date")))
  dfWithYear
    .groupBy("Year")
    .max()
    .select($"Year", $"max(High)")
    .orderBy("Year")
    .show(10)

  println("average close per month")
  val dfWithMonth = df.withColumn("Month", month(df("Date")))
  dfWithMonth
    .groupBy("Month")
    .mean()
    .select($"Month", $"avg(Close)")
    .show(4)
}
