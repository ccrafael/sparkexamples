ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.9"

lazy val root = (project in file("."))
  .settings(
    name := "sparkexamples"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % Test