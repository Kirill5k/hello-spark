name := "hello-spark"

version := "0.1"

scalaVersion := "2.12.9"

lazy val sparkVersion = "2.4.4"
lazy val scalaTestVersion = "3.0.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",

  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
