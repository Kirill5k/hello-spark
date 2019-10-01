package rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object StackexchangeQuestions extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "stackexchange")


  sc.stop()
}
