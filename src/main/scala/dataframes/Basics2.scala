package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType}

object Basics2 extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("Basics2")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val mobilePhonesSchema = StructType(Array(
    StructField("make", StringType),
    StructField("model", StringType),
    StructField("colour", StringType),
    StructField("manufacturerColour", StringType),
    StructField("storageCapacity", StringType),
    StructField("network", StringType),
    StructField("condition", StringType),
    StructField("listingTitle", StringType),
    StructField("datePosted", StringType),
    StructField("url", StringType),
    StructField("img", StringType),
    StructField("price", StringType),
    StructField("resellPrice", StringType),
    StructField("mpn", StringType)
  ))

  val mobilePhonesRows = Seq(
    Row(
      "Samsung", "Galaxy S8", "Black", "Midnight Black", "64GB", "O2", "Faulty",
      "Samsung Galaxy S8 Orchid Grey (O2) Smartphone", "2019-11-10T11:24:01.849+00:00",
      "https://www.ebay.co.uk/itm/Samsung-Galaxy-S8-Orchid-Grey-O2-Smartphone-/202819105162",
      "https://i.ebayimg.com/images/g/dIkAAOSwFEhdx~AR/s-l1600.jpg",
      "100.00", "126.00", "SMG950FZKABTU"
    )
  )

  val mobilePhonesData = session.sparkContext.parallelize(mobilePhonesRows)
  val mobilePhonesDf = session.createDataFrame(mobilePhonesData, mobilePhonesSchema)


  mobilePhonesDf.describe().show()
  mobilePhonesDf.printSchema()
  println(mobilePhonesDf.columns.toList)
  mobilePhonesDf.head(5).foreach(println)

  session.stop()
}
