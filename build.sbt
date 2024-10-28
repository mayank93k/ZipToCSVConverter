name := "ZIP to CSV Converter"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "com.typesafe" % "config" % "1.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.32.2",
  "org.scalatest" %% "scalatest" % "3.2.10",
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "org.slf4j" % "slf4j-log4j12" % "1.7.36",
  "log4j" % "log4j" % "1.2.17",
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.4",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.11.4",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.4"
).map(_.exclude("com.fasterxml.jackson.core", "jackson-databind"))

excludeDependencies ++= Seq(ExclusionRule("ch.qos.logoback", "logoback-classic"))