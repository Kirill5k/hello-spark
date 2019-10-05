package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object DatesAndTimestamps extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val customSchema = StructType(Array(
    StructField("Date", TimestampType, false),
    StructField("Open", DoubleType, false),
    StructField("High", DoubleType, false),
    StructField("Low", DoubleType, false),
    StructField("Close", DoubleType, false),
    StructField("Volume", IntegerType, false),
    StructField("Adj Close", DoubleType, false))
  )

  val session = SparkSession.builder()
    .appName("DatesAndTimestamps")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import session.implicits._

  val df = session.read
    .option("header", "true")
    .schema(customSchema)
    .csv("data/Netflix_2011_2016.csv")

  df.printSchema()
  df.show(5)

  import session.implicits._
  val df2 = df.withColumn("Year", year(df("Date")))
  val dfMeans = df2.groupBy("Year").mean()

  dfMeans.show(3)
  dfMeans.select($"Year", $"avg(Close)").show(5)
}
