package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")

  val session = SparkSession.builder()
    .appName("ColumnsAndExpressions")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val carsDf = session.read.option("inferSchema", "true").json("data/cars.json")
  val moreCarsDf = session.read.option("inferSchema", "true").json("data/more_cars.json")

  // Selecting (projecting) data from df:
  val carNamesDf = carsDf.select("Name")
  import session.implicits._
  val selectedColumnsDf = carsDf.select(
    carsDf.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    $"Horsepower",
    expr("Origin"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg")
  )

  // Expressions (transformations) of data
  val weightInIkExpression = carsDf.col("Weight_in_lbs") / 2.2
  val carsWithWeightsDf = carsDf.select(col("Name"), col("Weight_in_lbs"), weightInIkExpression.as("Weight_in_kg"))
  val carsWithSelectExprWeightsDf = carsDf.selectExpr("Name", "Weight_in_lbs", "Weight_in_lbs / 2.2")


  val carsDfWithNewCol = carsDf.withColumn("Weight_in_kg_2", col("Weight_in_lbs") / 2.2)
  val carsDfWithColRenamed = carsDf.withColumnRenamed("Weight_in_lbs", "Weight")

  // Filtering
  val filteredCarsDf = carsDf.filter(col("Origin") =!= "USA")
  val filteredCarsDf2 = carsDf.where(col("Origin") =!= "USA")
  val americanCarsDf = carsDf.filter("Origin = 'USA' and Horsepower > 150")
  val americanPowerfulCarsDf = carsDf.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDf2 = carsDf.filter(col("Origin") === "USA" and col("Horsepower") > 150)

  // adding data
  val allCarsDf = carsDf.union(moreCarsDf)

  session.stop()
}
