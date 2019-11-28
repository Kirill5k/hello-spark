name := "hello-spark"

version := "0.1"

scalaVersion := "2.11.12"

lazy val sparkVersion = "2.4.4"
lazy val sparkXmlVersion = "0.6.0"
lazy val scalaTestVersion = "3.0.8"
lazy val vegasVersion = "0.3.11"
lazy val postgresVersion = "42.2.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.databricks" %% "spark-xml" % sparkXmlVersion,

  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,

  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",

  "org.vegas-viz" %% "vegas" % vegasVersion,
  "org.vegas-viz" %% "vegas-spark" % vegasVersion,

  "org.postgresql" % "postgresql" % postgresVersion
)