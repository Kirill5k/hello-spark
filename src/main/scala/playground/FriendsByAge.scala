package playground

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val parseLine = (line: String) => {
    val fields = line.split(",")
    val age = fields(2).toInt
    val nFriends = fields(3).toInt
    (age, nFriends)
  }

  val sc = new SparkContext("local[*]", "friends-by-age")
  val linesRdd = sc.textFile("./data/fakefriends.csv")

  val averageByAge = linesRdd
    .map(parseLine)
    .mapValues(nFriends => (nFriends, 1))
    .reduceByKey{case ((n1, p1), (n2, p2)) => (n1+n2, p1+p2)}
    .mapValues{case (n, p) => n/p}
    .collect()

  averageByAge.map(_.swap).sorted.reverse.map(_.swap).foreach(println)

  sc.stop()
}
