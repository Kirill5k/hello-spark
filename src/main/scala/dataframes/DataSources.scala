package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType, nullable = false),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  Reading a DF:
  - format (i.e. json, csv)
  - schema (optional) or option("inferSchema", "true")
  - mode - failFast / dropMalformed / permissive (default)
   */
  val carsDf = session.read
    .schema(carsSchema)
    .format("json")
    .option("mode", "failFast")
    .option("dateFormat", "YYYY-MM-dd") // if spark fails parsing, null will be put
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .load("data/cars.json")

  /*
  Writing a DF:
  - format
  - save mode - overwrite / append / ignore / errorIfExists
   */
  carsDf.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("data/cars_dup.json")

  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  // CSV Flags
  val stocksDf = session.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .load("data/stocks.csv")

  // Parquet
  stocksDf.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .parquet("data/cars_parq.parquet")

  // Reading from DB
  val dbDf = session.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()
}
