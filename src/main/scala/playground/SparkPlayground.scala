package playground

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SparkPlayground extends App {

  val spark = SparkSession.builder()
    .appName("Spark Essentials Playground App")
    .config("spark.master", "local[*]")
    .getOrCreate()

  spark.stop()
}
