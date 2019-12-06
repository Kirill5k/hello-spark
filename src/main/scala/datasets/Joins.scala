package datasets

import datasets.Basics.Car
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Joins extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("DatasetJoins").config("spark.master", "local[*]").getOrCreate()

  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  import session.implicits._
  val guitarsDs = session.read.option("inferSchema", "true").json("data/guitars.json").as[Guitar]
  val guitarPlayersDs = session.read.option("inferSchema", "true").json("data/guitarPlayers.json").as[GuitarPlayer]
  val bandsDs = session.read.option("inferSchema", "true").json("data/bands.json").as[Band]

  val guitarPlayerBandsDs: Dataset[(GuitarPlayer, Band)] = guitarPlayersDs.joinWith(bandsDs, guitarPlayersDs.col("band") === bandsDs.col("id"), "inner")
  guitarPlayerBandsDs.show()

  val guitarsWithPlayersDs = guitarsDs.joinWith(guitarPlayersDs, array_contains(guitarPlayersDs.col("guitars"), guitarsDs.col("id")), "outer")
  guitarsWithPlayersDs.show()

  println("grouping by property")
  val carsDs = session.read.option("inferSchema", "true").json("data/cars.json").as[Car]
  val carsGroupedByOrigin = carsDs.groupByKey(_.Origin)
  carsGroupedByOrigin.count().show()
}
