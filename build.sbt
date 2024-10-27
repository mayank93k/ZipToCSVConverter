name := "Project_Spark_Scala"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "com.typesafe" % "config" % "1.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.32.2",
  "org.scalatest" %% "scalatest" % "3.2.10",
  "org.slf4j" % "slf4j-api" % "2.0.13",
  "org.slf4j" % "slf4j-log4j12" % "2.0.13",
  "log4j" % "log4j" % "1.2.17"
)

excludeDependencies ++= Seq(ExclusionRule("ch.qos.logoback", "logoback-classic"))