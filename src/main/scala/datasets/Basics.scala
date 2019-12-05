package datasets

import java.sql.Date
import java.time.{Instant, LocalDate}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Basics extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("DatasetsBasics").config("spark.master", "local[*]").getOrCreate()

  val numbersDf = session.read.option("header", "true").option("inferSchema", "true").csv("data/numbers.csv")
  numbersDf.printSchema()

  implicit val intEncoder = Encoders.scalaInt
  val numbersDs: Dataset[Int] = numbersDf.as[Int]

  // data of a complex type:
  final case class Car(Name: String, Miles_per_Gallon: Option[Double], Cylinders: Long, Displacement: Double, Horsepower: Option[Long], Weight_in_lbs: Long, Acceleration: Double, Year: Date, Origin: String)
  implicit val carEncoder = Encoders.product[Car]
  val carsDf = session.read.option("inferSchema", "true").json("data/cars.json")
  val carsDs = carsDf.as[Car]

  import session.implicits._
  println("total cars: ", carsDs.count())
  println("powerfull cars: ", carsDs.filter(_.Horsepower.getOrElse(0L) > 140).count())
  println("average hp: ", carsDs.map(_.Horsepower.getOrElse(0L)).reduce(_+_) / carsDs.count())
  println("average hp:", carsDs.select(avg(col("Horsepower"))).show())
}
