package xmls

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object XmlToJson extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder().appName("XmlToCsv").config("spark.master", "local[*]").getOrCreate()

  // https://github.com/databricks/spark-xml
  val df = session.read
    .format("com.databricks.spark.xml")
    .option("ignoreSurroundingSpaces", "true")
    .option("rowTag", "row")
    .load("data/stackexchange/Posts.xml")

  df.write
    .json("data/stackexchange/posts-json")
}
