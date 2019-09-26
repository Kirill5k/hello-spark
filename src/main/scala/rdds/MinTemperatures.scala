package rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MinTemperatures extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "MinTemperatures")

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temp)
  }

  val lines = sc.textFile("data/1800.csv")

  val minTempByStation = lines
    .map(parseLine)
    .filter{case(_, entryType, _) => entryType == "TMIN"}
    .map{case(id, _, temp) => (id, temp)}
    .reduceByKey(math.min)
    .sortBy(_._2)
    .collect()

  minTempByStation
    .foreach(println)

  sc.stop()
}
