package sql

import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

class Basics extends App {

  val session = SparkSession.builder()
    .appName("sql")
    .config("spark.master", "local[*]")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  var carsDf = session.read
    .option("inferSchema", "true")
    .json("data/cars.json")

  carsDf.createOrReplaceTempView("cars")

  val americanCarsDf = session.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  session.sql("create database test")
  session.sql("use test")
  val databasesDf = session.sql("show databases")


  val employeesDf = readTableFromDb("employees")
  employeesDf.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")

  def readTableFromDb(tableName: String): sql.DataFrame = session.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.${tableName}")
    .load()


  def transferTables(tableNames: Seq[String]) = tableNames.foreach { table =>
    val tableDf = readTableFromDb(table)
    tableDf.createOrReplaceTempView(table)
    tableDf.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(table)
  }

  val employeesDf2 = session.read.table("employees")
}
