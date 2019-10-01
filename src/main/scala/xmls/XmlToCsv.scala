package xmls

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}

object XmlToCsv extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder().appName("XmlToCsv").config("spark.master", "local[*]").getOrCreate()

  // https://github.com/databricks/spark-xml
  val df = session.read
    .format("com.databricks.spark.xml")
    .option("ignoreSurroundingSpaces", "true")
    .option("rowTag", "row")
    .option("attributePrefix", "")
    .load("data/stackexchange/Posts.xml")

  df.write
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .save("data/stackexchange/Posts-csv")
}
