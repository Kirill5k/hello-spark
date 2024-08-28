ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.11"

lazy val sparkVersion = "2.4.4"
//lazy val sparkVersion = "3.0.0"
lazy val sparkXmlVersion = "0.6.0"
lazy val scalaTestVersion = "3.0.8"
lazy val vegasVersion = "0.3.11"
lazy val postgresVersion = "42.2.2"

lazy val root = project.in(file("."))
  .settings(
    name := "hello-spark",
    libraryDependencies ++= Seq(
      "org.xerial.snappy" % "snappy-java" % "1.1.8.4",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "com.databricks" %% "spark-xml" % sparkXmlVersion,

      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,

      "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.4.1",

      "org.postgresql" % "postgresql" % postgresVersion
    ),
    Compile / mainClass := Some("PoModelTest")
  )