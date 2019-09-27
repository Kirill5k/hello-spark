package rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularMarvelHero extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "MostPopularMarvelHero")

  val names = sc.textFile("data/Marvel-names.txt")
    .map(_.split(" ", 2))
    .map(fields => (fields(0).toInt, fields(1).replace("\"", "")))
    .collectAsMap()

  val namesDict = sc.broadcast(names)
  val relationshipGraph = sc.textFile("data/Marvel-graph.txt")

  val friendsCount = relationshipGraph
    .map(_.split("\\s+"))
    .map(fields => (fields(0).toInt, fields.length))
    .reduceByKey(_ + _)
    .sortBy(_._2, false)
    .collect()

  friendsCount
    .map{case(id, nFriends) => (namesDict.value(id), nFriends)}
    .foreach(println)

  namesDict.destroy()
  sc.stop()
}
