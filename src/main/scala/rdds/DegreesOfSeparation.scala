package rdds

import java.awt.Color

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DegreesOfSeparation extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "degrees-of-separation")

  type BfsData = (Seq[Int], Int, Color)
  type BfsNode = (Int, BfsData)

  val startCharacterId = 5306
  val targetCharacterId = 14

  val hitCounter = sc.longAccumulator("hit-counter")

  val heroGraph: RDD[BfsNode] = sc.textFile("data/Marvel-graph.txt")
    .map(_.split("\\s+").map(_.toInt))
    .map{
      case Array(id, connections @ _*) if id == startCharacterId => (id, (connections, 0, Color.GRAY))
      case Array(id, connections @ _*) => (id, (connections, Int.MaxValue, Color.WHITE))
    }


  def bfsMap(node: BfsNode): Seq[BfsNode] = {
    val (id, (connections, distance, color)) = node

    val updatedEntry: BfsNode = (id, (connections, distance, Color.BLACK))

    if (color == Color.GRAY) {
      connections.map(conn => {
        if (targetCharacterId == conn) hitCounter.add(1)
        (conn, (Seq(), distance+1, Color.GRAY))
      }) ++ (updatedEntry +: Seq())
    } else {
      updatedEntry +: Seq()
    }
  }

  def bfsReduce(data1: BfsData, data2: BfsData): BfsData = {
    val (connections1, dist1, c1) = data1
    val (connections2, dist2, c2) = data2

    val updatedColor = if (c1 == Color.BLACK || c2 == Color.BLACK) Color.BLACK else Color.WHITE
    (connections1 ++ connections2, math.min(dist1, dist2), updatedColor)
  }
}
