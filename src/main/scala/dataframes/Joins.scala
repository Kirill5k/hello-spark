package dataframes

import dataframes.Operations2.session
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max,expr}

object Joins extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("Groupings").config("spark.master", "local[*]").getOrCreate()

  val guitarsDf = session.read.option("inferSchema", "true").json("data/guitars.json")
  val guitarPlayers = session.read.option("inferSchema", "true").json("data/guitarPlayers.json")
  val bandsDf = session.read.option("inferSchema", "true").json("data/bands.json")

  val playersAndBandsJoin = guitarPlayers.col("band") === bandsDf.col("id")
  // inner joins
  val guitarBands = guitarPlayers.join(bandsDf, playersAndBandsJoin, "inner")

  println("inner joins")
  guitarBands.show()
  println()

  // outer joins
  println("left outer = everything in the inner join + all the rows in the LEFT table")
  guitarPlayers.join(bandsDf, playersAndBandsJoin, "left_outer").show()
  println()

  println("right outer = everything in the inner join + all the rows in the RIGHT table")
  guitarPlayers.join(bandsDf, playersAndBandsJoin, "right_outer").show()
  println()

  println("(full) outer join = everything in the inner join + all the rows in BOTH tables")
  guitarPlayers.join(bandsDf, playersAndBandsJoin, "outer").show()
  println()

  println("semi join = data from the left table with matching data from right table cut out")
  guitarPlayers.join(bandsDf, playersAndBandsJoin, "left_semi").show()
  println()

  println("anti join = data with missing rows")
  guitarPlayers.join(bandsDf, playersAndBandsJoin, "left_anti").show()
  println()

  println("renaming columns")
  guitarPlayers.join(bandsDf.withColumnRenamed("id", "band"), "band").show()
  println()

  println("using complex types")
  guitarPlayers.join(guitarsDf.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show

  // get all employees and max salaries - salary table - emp_no, salary,
  // show all employees who were never managers dept_manager table
  // find job titles of best paid 10 employees - titles table emp_no, title, from_date, to_date

  def readTableFromDb(tableName: String): sql.DataFrame = session.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", s"public.${tableName}")
      .load()

  val employeesDf = readTableFromDb("employees")
  val salariesDf = readTableFromDb("salaries")
  val managersDf = readTableFromDb("dept_manager")
  val jobTitlesDf = readTableFromDb("titles")

  import session.implicits._
  val employeesWithSalaries = employeesDf.join(salariesDf, "emp_no")
  val maxEmployeesSalaries = employeesWithSalaries.groupBy("emp_no").agg(max("salary"))

  val employeesWhoWereNeverNotManagers = employeesDf.join(managersDf, employeesDf.col("emp_no") === employeesDf.col("emp_no"), "left_anti")

  val joinCond = employeesWithSalaries.col("to_date") === jobTitlesDf.col("to_date") and employeesWithSalaries.col("id") === jobTitlesDf.col("emp_no")
  val employeesWithPositionsAndSalaries = employeesWithSalaries.join(jobTitlesDf, joinCond, "inner")
  val topSalariesEmployees = employeesWithPositionsAndSalaries.groupBy("emp_no", "title").agg(max("to_date")).orderBy($"salary".desc).limit(10)
}
