package rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordsCounter extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "words-counter")

  val book = sc.textFile("data/book.txt")

  val words = book
    .flatMap(_.split("\\s+"))
    .map((_, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, false)
    .collect()

  words.foreach(println)

  sc.stop()
}
